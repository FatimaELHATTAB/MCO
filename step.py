# src/matching_engine/matching_table.py
from __future__ import annotations

import hashlib
import logging
from datetime import date
from typing import Dict, List, Set, Tuple, Optional, Iterable, Union

import polars as pl


FrameLike = Union[pl.DataFrame, pl.LazyFrame]


class MatchingTable:
    """
    Canonical manager for t_matching_entity:
      - load current open rows (optionally by provider/country)
      - format raw pipeline output to canonical schema
      - merge current + new to derive open/close/unchanged
      - (re)compute clusters on active rows (connected components)
      - write changes (insert/close)
    All public methods accept either Polars DataFrame or LazyFrame.
    """

    # ---------- lifecycle ----------

    def __init__(self, db_connection):
        """
        :param db_connection: object exposing `.conn` (PEP-249 connection) and `.cursor`
        """
        self.conn = getattr(db_connection, "conn", db_connection)
        self.cursor = getattr(db_connection, "cursor", db_connection.cursor)
        self.today = date.today()
        self.logger = logging.getLogger(__name__)

    # ---------- schema ----------

    @staticmethod
    def _schema() -> Dict[str, pl.DataType]:
        return {
            "intern_id": pl.Utf8,
            "target_id": pl.Utf8,
            "intern_name": pl.Utf8,
            "target_name": pl.Utf8,
            "matching_rule": pl.Utf8,
            "matching_start_date": pl.Date,
            "cluster_id": pl.Utf8,
            "matching_end_date": pl.Date,
            "closure_reason": pl.Utf8,
            "is_duplicate": pl.Boolean,
            "lisbon_decision": pl.Boolean,
            "lisbon_date": pl.Date,
            "lisbon_user": pl.Utf8,
            "confidence_score": pl.Utf8,
            "country": pl.Utf8,
            "provider": pl.Utf8,         # <- important pour filtrer/écrire correctement
        }

    # ---------- helpers (lazy-safe) ----------

    @staticmethod
    def _to_df(obj: FrameLike) -> pl.DataFrame:
        if isinstance(obj, pl.LazyFrame):
            return obj.collect()
        if isinstance(obj, pl.DataFrame):
            return obj
        raise TypeError("Expected Polars DataFrame or LazyFrame")

    def ensure_schema(self, df_like: FrameLike) -> pl.DataFrame:
        df = self._to_df(df_like)
        schema = self._schema()
        # add missing columns
        for col, dtype in schema.items():
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).cast(dtype).alias(col))
        # keep order & cast
        df = df.select(list(schema.keys()))
        for col, dtype in schema.items():
            df = df.with_columns(pl.col(col).cast(dtype, strict=False))
        return df

    # ---------- DB IO ----------

    def load_provider_data(self, provider_id: str, country: Optional[str] = None) -> pl.DataFrame:
        """
        Load OPEN rows for a provider (and optional country). Returns canonical schema.
        """
        q = """
            SELECT
              intern_id::text   AS intern_id,
              target_id::text   AS target_id,
              intern_name::text AS intern_name,
              target_name::text AS target_name,
              matching_rule::text AS matching_rule,
              matching_start_date,
              cluster_id::text  AS cluster_id,
              matching_end_date,
              closure_reason::text AS closure_reason,
              is_duplicate,
              lisbon_decision,
              lisbon_date,
              lisbon_user::text AS lisbon_user,
              confidence_score::text AS confidence_score,
              country::text     AS country,
              provider::text    AS provider
            FROM t_matching_entity
            WHERE provider = %s
              AND matching_end_date IS NULL
              AND (closure_reason IS NULL OR closure_reason = '')
        """
        params: Tuple = (provider_id,)
        if country:
            q += " AND country = %s"
            params = (provider_id, country)

        self.cursor.execute(q, params)
        rows = self.cursor.fetchall()
        if not rows:
            return self.ensure_schema(pl.DataFrame(schema=self._schema()))

        cols = [d[0] for d in self.cursor.description]
        df = pl.from_records(rows, columns=cols)
        return self.ensure_schema(df)

    # ---------- cluster utils ----------

    @staticmethod
    def _hash_cluster(nodes: List[str], provider: str) -> str:
        combined = "|".join(sorted(set(nodes))) + f"::prov={provider}"
        return hashlib.sha256(combined.encode("utf-8")).hexdigest()

    @staticmethod
    def _build_graph(edges: pl.DataFrame) -> Dict[str, Set[str]]:
        graph: Dict[str, Set[str]] = {}
        for a, b in edges.select(["intern_id", "target_id"]).iter_rows():
            a, b = str(a), str(b)
            graph.setdefault(a, set()).add(b)
            graph.setdefault(b, set()).add(a)
        return graph

    @classmethod
    def _connected_components(cls, edges: pl.DataFrame, provider: str) -> Dict[str, str]:
        graph = cls._build_graph(edges)
        visited: Set[str] = set()
        mapping: Dict[str, str] = {}

        for node in graph:
            if node in visited:
                continue
            stack = [node]
            comp: List[str] = []
            while stack:
                cur = stack.pop()
                if cur in visited:
                    continue
                visited.add(cur)
                comp.append(cur)
                stack.extend(graph.get(cur, ()))
            cid = cls._hash_cluster(comp, provider)
            for n in comp:
                mapping[n] = cid
        return mapping

    def recalculate_clusters(self, table_like: FrameLike, provider: str) -> pl.DataFrame:
        """
        Recompute cluster_id on active rows (end_date NULL). Mark duplicates where cluster size > 1.
        """
        table = self.ensure_schema(table_like)
        active = table.filter(pl.col("matching_end_date").is_null())
        if active.is_empty():
            return table

        edges = active.select(["intern_id", "target_id"]).unique()
        mapping = self._connected_components(edges, provider)

        updated = table.with_columns(
            pl.col("intern_id").map_elements(lambda x: mapping.get(x), return_dtype=pl.Utf8).alias("cluster_id")
        )

        counts = (
            updated.filter(pl.col("matching_end_date").is_null())
            .group_by("cluster_id")
            .len()
            .rename({"len": "n"})
        )
        updated = updated.join(counts, on="cluster_id", how="left").with_columns(
            (pl.col("n") > 1).alias("is_duplicate")
        ).drop("n")

        return self.ensure_schema(updated)

    # ---------- join + diffs ----------

    @staticmethod
    def _outer_join_current_new(current: pl.DataFrame, new: pl.DataFrame) -> pl.DataFrame:
        cast_cur = current.with_columns(pl.col("intern_id").cast(pl.Utf8), pl.col("target_id").cast(pl.Utf8))
        cast_new = new.with_columns(pl.col("intern_id").cast(pl.Utf8), pl.col("target_id").cast(pl.Utf8))
        joined = cast_cur.join(cast_new, on=["intern_id", "target_id"], how="outer", suffix="_new")
        if "matching_rule" in cast_cur.columns:
            joined = joined.with_columns(pl.col("matching_rule").alias("matching_rule_current"))
        # ensure expected companions exist
        for col in ["intern_name", "target_name", "matching_rule", "cluster_id", "country", "provider"]:
            if col not in joined.columns:
                joined = joined.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
            if f"{col}_new" not in joined.columns:
                joined = joined.with_columns(pl.lit(None).cast(pl.Utf8).alias(f"{col}_new"))
        return joined

    def _to_close(self, joined: pl.DataFrame) -> pl.DataFrame:
        cond_removed = (
            (pl.col("matching_rule").is_not_null())
            & (
                (pl.col("matching_rule_new").is_null())
                | (pl.col("target_id").is_not_null() & (pl.col("target_id_new") != pl.col("target_id")))
                | (pl.col("matching_rule_current").is_not_null()
                   & (pl.col("matching_rule_new") != pl.col("matching_rule_current")))
            )
        )
        return (
            joined.filter(cond_removed)
            .select([
                "intern_id", "target_id",
                "intern_name", "target_name",
                "matching_rule", "cluster_id",
                pl.lit(self.today).alias("matching_end_date").cast(pl.Date),
                pl.lit("RULE_CHANGED").alias("closure_reason"),
                pl.lit(False).alias("is_duplicate").cast(pl.Boolean),
                pl.lit(None).cast(pl.Boolean).alias("lisbon_decision"),
                pl.lit(None).cast(pl.Date).alias("lisbon_date"),
                pl.lit(None).cast(pl.Utf8).alias("lisbon_user"),
                pl.lit(None).cast(pl.Utf8).alias("confidence_score"),
                pl.coalesce([pl.col("country"), pl.col("country_new")]).alias("country"),
                pl.coalesce([pl.col("provider"), pl.col("provider_new")]).alias("provider"),
            ])
        )

    def _to_new(self, joined: pl.DataFrame) -> pl.DataFrame:
        cond_new = (
            (pl.col("matching_rule_new").is_not_null())
            & (
                pl.col("matching_rule").is_null()
                | (pl.col("target_id").is_null())
                | (pl.col("target_id_new") != pl.col("target_id"))
                | (pl.col("matching_rule_current").is_null())
                | (pl.col("matching_rule_new") != pl.col("matching_rule_current"))
            )
        )
        return (
            joined.filter(cond_new)
            .select([
                "intern_id",
                pl.col("target_id_new").alias("target_id"),
                pl.col("intern_name_new").alias("intern_name"),
                pl.col("target_name_new").alias("target_name"),
                pl.col("matching_rule_new").alias("matching_rule"),
                pl.lit(self.today).alias("matching_start_date").cast(pl.Date),
                pl.col("cluster_id_new").alias("cluster_id"),
                pl.lit(None).cast(pl.Date).alias("matching_end_date"),
                pl.lit(None).cast(pl.Utf8).alias("closure_reason"),
                pl.lit(False).alias("is_duplicate").cast(pl.Boolean),
                pl.lit(None).cast(pl.Boolean).alias("lisbon_decision"),
                pl.lit(None).cast(pl.Date).alias("lisbon_date"),
                pl.lit(None).cast(pl.Utf8).alias("lisbon_user"),
                pl.lit(None).cast(pl.Utf8).alias("confidence_score"),
                pl.coalesce([pl.col("country"), pl.col("country_new")]).alias("country"),
                pl.coalesce([pl.col("provider"), pl.col("provider_new")]).alias("provider"),
            ])
        )

    def _unchanged(self, joined: pl.DataFrame) -> pl.DataFrame:
        cond_same = (
            (pl.col("matching_rule_new").is_not_null())
            & (pl.col("matching_rule_current") == pl.col("matching_rule_new"))
            & (pl.col("target_id") == pl.col("target_id_new"))
        )
        keep = list(self._schema().keys())
        return joined.filter(cond_same).select([c if c in joined.columns else pl.lit(None).alias(c) for c in keep])

    # ---------- merge entry point ----------

    def update_table(self, current_like: FrameLike, new_like: FrameLike) -> pl.DataFrame:
        """
        Merge current + new, produce a full table (open + recently closed).
        """
        current = self.ensure_schema(current_like)
        new = self.ensure_schema(new_like)

        joined = self._outer_join_current_new(current, new)
        to_close = self._to_close(joined)
        to_new = self._to_new(joined)
        same = self._unchanged(joined)

        updated = pl.concat([to_close, to_new, same], how="diagonal_relaxed")
        return self.ensure_schema(updated)

    # ---------- format from pipeline ----------

    def format_matching_output(
        self,
        df_like: FrameLike,
        *,
        provider: str,
        default_country: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Adapt raw pipeline matches to canonical schema.
        Expects at least (case-insensitive fallback applied via safe_col):
          left_id/right_id -> intern_id/target_id
          rule_name or matching_rule
          optional: intern_name, target_name, confidence_score, country
        """
        dfm = self._to_df(df_like)
        if dfm.is_empty():
            return self.ensure_schema(dfm)

        def safe_col(choices: Iterable[str], default=None):
            for c in choices:
                if c in dfm.columns:
                    return pl.col(c)
            return pl.lit(default)

        out = dfm.with_columns(
            safe_col(["left_id", "intern_id", "ID_INTRN"]).cast(pl.Utf8).alias("intern_id"),
            safe_col(["right_id", "target_id", "TGT_ID"]).cast(pl.Utf8).alias("target_id"),
            safe_col(["intern_name"]).cast(pl.Utf8).alias("intern_name"),
            safe_col(["target_name"]).cast(pl.Utf8).alias("target_name"),
            safe_col(["matching_rule", "rule_name"]).cast(pl.Utf8).alias("matching_rule"),
            pl.lit(self.today).cast(pl.Date).alias("matching_start_date"),
            safe_col(["cluster_id"]).cast(pl.Utf8).alias("cluster_id"),
            pl.lit(None).cast(pl.Date).alias("matching_end_date"),
            pl.lit(None).cast(pl.Utf8).alias("closure_reason"),
            pl.lit(False).cast(pl.Boolean).alias("is_duplicate"),
            pl.lit(None).cast(pl.Boolean).alias("lisbon_decision"),
            pl.lit(None).cast(pl.Date).alias("lisbon_date"),
            pl.lit(None).cast(pl.Utf8).alias("lisbon_user"),
            safe_col(["confidence_score", "score"]).cast(pl.Utf8).alias("confidence_score"),
            (safe_col(["country"]).cast(pl.Utf8) if "country" in dfm.columns else pl.lit(default_country).cast(pl.Utf8)).alias("country"),
            pl.lit(provider).cast(pl.Utf8).alias("provider"),
        ).select(list(self._schema().keys()))

        return self.ensure_schema(out)

    # ---------- DB write ----------

    def insert_rows(self, rows_like: FrameLike, batch_size: int = 1000) -> None:
        rows = self.ensure_schema(rows_like)
        if rows.is_empty():
            self.logger.info("insert_rows: nothing to insert.")
            return

        q = """
            INSERT INTO t_matching_entity (
              intern_id, target_id, intern_name, target_name,
              matching_rule, matching_start_date, cluster_id,
              matching_end_date, closure_reason, is_duplicate,
              lisbon_decision, lisbon_date, lisbon_user,
              confidence_score, country, provider
            ) VALUES (
              %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            ) ON CONFLICT DO NOTHING
        """
        cols = list(self._schema().keys())

        n = rows.height
        for i in range(0, n, batch_size):
            batch = rows.slice(i, min(batch_size, n - i))
            payload = [tuple(batch.select(cols).row(j)) for j in range(batch.height)]
            self.cursor.executemany(q, payload)
            self.conn.commit()
            self.logger.info(f"Inserted {(i//batch_size)+1} / {((n-1)//batch_size)+1} (rows={len(payload)})")

    def close_rows(
        self,
        to_close_like: FrameLike,
        *,
        provider: str,
        reason: str = "RULE_CHANGED",
        batch_size: int = 1000,
    ) -> None:
        to_close = self._to_df(to_close_like)
        if to_close.is_empty():
            self.logger.info("close_rows: nothing to close.")
            return

        # Close by (provider, intern_id, target_id)
        q = """
            UPDATE t_matching_entity
            SET matching_end_date = %s,
                closure_reason = %s
            WHERE provider = %s
              AND intern_id = %s
              AND target_id = %s
              AND matching_end_date IS NULL
        """
        params = [
            (self.today, reason, provider, a, b)
            for a, b in to_close.select(["intern_id", "target_id"]).iter_rows()
        ]

        for i in range(0, len(params), batch_size):
            chunk = params[i:i+batch_size]
            self.cursor.executemany(q, chunk)
            self.conn.commit()
            self.logger.info(f"Closed {(i//batch_size)+1} / {((len(params)-1)//batch_size)+1} (rows={len(chunk)})")

    # ---------- high-level orchestration ----------

    def merge_write_recluster(
        self,
        *,
        provider: str,
        current_open_like: FrameLike,
        new_like: FrameLike,
    ) -> pl.DataFrame:
        """
        1) merge current open + new -> updated full table
        2) insert new rows; close removed rows
        3) recluster and return updated table (in-memory)
        """
        current = self.ensure_schema(current_open_like)
        new = self.ensure_schema(new_like)

        joined = self._outer_join_current_new(current, new)
        to_close = self._to_close(joined)
        to_new = self._to_new(joined)

        if not to_new.is_empty():
            self.insert_rows(to_new)
        if not to_close.is_empty():
            self.close_rows(to_close, provider=provider, reason="RULE_CHANGED")

        # recompute clusters on fresh snapshot (current + applied delta)
        snapshot = self.load_provider_data(provider)  # reload open after writes
        reclustered = self.recalculate_clusters(snapshot, provider=provider)

        # Optionally persist cluster_id to DB (single pass by intern_id)
        self._persist_clusters(reclustered, provider)

        return reclustered

    def _persist_clusters(self, updated_open: pl.DataFrame, provider: str) -> None:
        """
        Persist cluster_id for active rows of provider (by intern_id).
        """
        active = updated_open.filter(pl.col("matching_end_date").is_null() & pl.col("cluster_id").is_not_null())
        if active.is_empty():
            return

        # Build a VALUES list (intern_id, cluster_id)
        values = ",".join(["(%s,%s)"] * active.height)
        q = f"""
        UPDATE t_matching_entity AS t
        SET cluster_id = v.cluster_id
        FROM (VALUES {values}) AS v(intern_id, cluster_id)
        WHERE t.provider = %s
          AND t.matching_end_date IS NULL
          AND t.intern_id = v.intern_id
        """
        params_list = list(active.select(["intern_id", "cluster_id"]).iter_rows())
        flat: List = []
        for a, b in params_list:
            flat += [a, b]
        flat.append(provider)

        self.cursor.execute(q, flat)
        self.conn.commit()
