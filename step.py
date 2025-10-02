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
    Public methods accept Polars DataFrame or LazyFrame.
    """

    def __init__(self, db_connection):
        self.conn = getattr(db_connection, "conn", db_connection)
        self.cursor = getattr(db_connection, "cursor", db_connection.cursor)
        self.today = date.today()
        self.logger = logging.getLogger(__name__)

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
            "provider": pl.Utf8,
        }

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
        for col, dtype in schema.items():
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).cast(dtype).alias(col))
        df = df.select(list(schema.keys()))
        for col, dtype in schema.items():
            df = df.with_columns(pl.col(col).cast(dtype, strict=False))
        return df

    def load_provider_data(self, provider_id: str, country: Optional[str] = None) -> pl.DataFrame:
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
        return self.ensure_schema(pl.from_records(rows, columns=cols))

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
            .group_by("cluster_id").len().rename({"len": "n"})
        )
        updated = updated.join(counts, on="cluster_id", how="left").with_columns(
            (pl.col("n") > 1).alias("is_duplicate")
        ).drop("n")
        return self.ensure_schema(updated)

    @staticmethod
    def _outer_join_current_new(current: pl.DataFrame, new: pl.DataFrame) -> pl.DataFrame:
        cast_cur = current.with_columns(pl.col("intern_id").cast(pl.Utf8), pl.col("target_id").cast(pl.Utf8))
        cast_new = new.with_columns(pl.col("intern_id").cast(pl.Utf8), pl.col("target_id").cast(pl.Utf8))
        joined = cast_cur.join(cast_new, on=["intern_id", "target_id"], how="outer", suffix="_new")
        if "matching_rule" in cast_cur.columns:
            joined = joined.with_columns(pl.col("matching_rule").alias("matching_rule_current"))
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

    def update_table(self, current_like: FrameLike, new_like: FrameLike) -> pl.DataFrame:
        current = self.ensure_schema(current_like)
        new = self.ensure_schema(new_like)
        joined = self._outer_join_current_new(current, new)
        to_close = self._to_close(joined)
        to_new = self._to_new(joined)
        same = joined.filter(
            (pl.col("matching_rule_new").is_not_null())
            & (pl.col("matching_rule_current") == pl.col("matching_rule_new"))
            & (pl.col("target_id") == pl.col("target_id_new"))
        ).select(list(self._schema().keys()))
        updated = pl.concat([to_close, to_new, same], how="diagonal_relaxed")
        return self.ensure_schema(updated)

    def insert_rows(self, rows_like: FrameLike, batch_size: int = 1000) -> None:
        rows = self.ensure_schema(rows_like)
        if rows.is_empty():
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

    def close_rows(self, to_close_like: FrameLike, *, provider: str, reason: str = "RULE_CHANGED", batch_size: int = 1000) -> None:
        to_close = self._to_df(to_close_like)
        if to_close.is_empty():
            return
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

    def _persist_clusters(self, updated_open: pl.DataFrame, provider: str) -> None:
        active = updated_open.filter(pl.col("matching_end_date").is_null() & pl.col("cluster_id").is_not_null())
        if active.is_empty():
            return
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





from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple, Set

import polars as pl
from matching_engine.db.db_client import DbClient
from matching_engine.matching_table import MatchingTable


@dataclass(frozen=True)
class FICPairs:
    provider: str
    df: pl.DataFrame  # columns: intern_id, target_id (Utf8)


class FICSynchronizerLazy:
    """
    Synchronize FIC vs t_matching_entity for a provider in a Lazy-friendly way:
      - load reference pairs from t_ref_fic_client (by provider)
      - load current OPEN rows from t_matching_entity (any rule) for provider
      - compute deltas:
          to_insert: in ref, not present at all
          to_upgrade: present but non-FIC -> close old + insert FIC
          to_close: FIC present but no longer in ref
      - write changes
      - reload snapshot, recluster, persist cluster ids
      - return active FIC pairs (for prefilter)
    """

    def __init__(self, db: DbClient, mtbl: MatchingTable,
                 t_fic_ref: str = "t_ref_fic_client", t_match: str = "t_matching_entity"):
        self.db = db
        self.mtbl = mtbl
        self.t_fic_ref = t_fic_ref
        self.t_match = t_match

    # --- loaders (Lazy) ---

    def load_reference_pairs(self, provider: str) -> pl.LazyFrame:
        q = f"""
        SELECT
          S3DB_id::text AS intern_id,
          tgt_id::text  AS target_id,
          provider::text AS provider
        FROM {self.t_fic_ref}
        WHERE provider = %s
        """
        return self.db.fetch_lazy(q, (provider,))

    def load_current_open(self, provider: str) -> pl.LazyFrame:
        q = f"""
        SELECT
          intern_id::text   AS intern_id,
          target_id::text   AS target_id,
          target_name::text AS target_name,
          matching_rule::text AS matching_rule,
          matching_start_date,
          cluster_id::text  AS cluster_id,
          matching_end_date,
          closure_reason::text AS closure_reason,
          country::text     AS country,
          confidence_score::text AS confidence_score,
          provider::text    AS provider
        FROM {self.t_match}
        WHERE provider = %s
          AND matching_end_date IS NULL
        """
        return self.db.fetch_lazy(q, (provider,))

    # --- diffs (eager on purpose for DB writes) ---

    @staticmethod
    def _pairs_set(df: pl.DataFrame) -> Set[tuple[str, str]]:
        if df.is_empty():
            return set()
        return set((a, b) for a, b in df.select(["intern_id", "target_id"]).iter_rows())

    def compute_deltas(self, provider: str) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        ref_df = self.load_reference_pairs(provider).collect().select(["intern_id", "target_id"]).unique()
        cur_df = self.load_current_open(provider).collect()

        # current all open (any rule), current FIC subset
        cur_pairs = self._pairs_set(cur_df)
        cur_fic_pairs = self._pairs_set(cur_df.filter(pl.col("matching_rule") == "FIC"))
        ref_pairs = self._pairs_set(ref_df)

        to_insert_pairs = ref_pairs - cur_pairs
        to_upgrade_pairs = (ref_pairs & cur_pairs) - cur_fic_pairs
        to_close_pairs = cur_fic_pairs - ref_pairs

        def df_pairs(s: Set[tuple[str, str]]) -> pl.DataFrame:
            if not s:
                return pl.DataFrame({"intern_id": pl.Series([], dtype=pl.Utf8),
                                     "target_id": pl.Series([], dtype=pl.Utf8),
                                     "provider": pl.Series([], dtype=pl.Utf8)})
            return pl.DataFrame([{"intern_id": a, "target_id": b, "provider": provider} for a, b in s])

        return df_pairs(to_insert_pairs), df_pairs(to_upgrade_pairs), df_pairs(to_close_pairs)

    # --- apply deltas ---

    def _insert_fic_rows(self, pairs: pl.DataFrame, provider: str) -> None:
        if pairs.is_empty():
            return
        rows = pairs.with_columns(
            pl.lit(None).alias("intern_name"),
            pl.lit(provider).alias("target_name"),
            pl.lit("FIC").alias("matching_rule"),
            pl.lit(self.mtbl.today).cast(pl.Date).alias("matching_start_date"),
            pl.lit(None).alias("cluster_id"),
            pl.lit(None).cast(pl.Date).alias("matching_end_date"),
            pl.lit(None).alias("closure_reason"),
            pl.lit(False).cast(pl.Boolean).alias("is_duplicate"),
            pl.lit(None).cast(pl.Boolean).alias("lisbon_decision"),
            pl.lit(None).cast(pl.Date).alias("lisbon_date"),
            pl.lit(None).alias("lisbon_user"),
            pl.lit(None).alias("confidence_score"),
            pl.lit(None).alias("country"),
        )
        self.mtbl.insert_rows(rows)

    def _close_pairs(self, pairs: pl.DataFrame, provider: str, reason: str) -> None:
        if pairs.is_empty():
            return
        self.mtbl.close_rows(pairs.select(["intern_id", "target_id"]), provider=provider, reason=reason)

    def sync_provider(self, provider: str) -> FICPairs:
        """
        Full sync for one provider.
        """
        to_insert, to_upgrade, to_close = self.compute_deltas(provider)

        if not to_close.is_empty():
            self._close_pairs(to_close, provider, reason="FIC_REMOVED")

        if not to_upgrade.is_empty():
            # close old line (non-FIC) then insert as FIC
            self._close_pairs(to_upgrade, provider, reason="FIC_UPGRADE")
            self._insert_fic_rows(to_upgrade, provider)

        if not to_insert.is_empty():
            self._insert_fic_rows(to_insert, provider)

        # Reload snapshot (open rows), recluster, persist cluster_ids
        snapshot = self.mtbl.load_provider_data(provider)
        reclustered = self.mtbl.recalculate_clusters(snapshot, provider=provider)
        self.mtbl._persist_clusters(reclustered, provider)

        # Return current active FIC pairs for prefilter
        active_fic = reclustered.filter(pl.col("matching_rule") == "FIC").select(["intern_id", "target_id"])
        return FICPairs(provider=provider, df=active_fic)







from __future__ import annotations
from dataclasses import dataclass
import polars as pl

@dataclass(frozen=True)
class FICPairs:
    provider: str
    df: pl.DataFrame  # intern_id, target_id

def apply_fic_prefilter(
    left_lf: pl.LazyFrame,
    right_lf: pl.LazyFrame,
    fic_pairs: FICPairs,
    *,
    left_id: str,
    right_id: str,
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Exclude any left/right rows whose IDs appear in FIC pairs (anti-join Lazy).
    """
    if fic_pairs.df.is_empty():
        return left_lf, right_lf

    fic_left_ids  = fic_pairs.df.select(pl.col("intern_id").alias(left_id)).unique().lazy()
    fic_right_ids = fic_pairs.df.select(pl.col("target_id").alias(right_id)).unique().lazy()

    left_clean  = left_lf.join(fic_left_ids,  on=left_id,  how="anti")
    right_clean = right_lf.join(fic_right_ids, on=right_id, how="anti")
    return left_clean, right_clean





from __future__ import annotations
import polars as pl

from matching_engine.db.db_client import DbClient
from matching_engine.matching_table import MatchingTable
from matching_engine.fic.fic_sync_lazy import FICSynchronizerLazy
from matching_engine.fic.prefilter import apply_fic_prefilter

# + tes imports pipeline: UnifiedLazyLoader, MatchingPipelineLazy, enrich_matches_for_table, etc.

def run_provider(db: DbClient, provider: str, left_lf: pl.LazyFrame, right_lf: pl.LazyFrame,
                 left_id: str, right_id: str):
    """
    Exemple minimal : FIC sync + prefilter, puis ta pipeline (non incluse ici pour focus FIC).
    """
    mtbl = MatchingTable(db)
    fic_sync = FICSynchronizerLazy(db, mtbl)

    # 1) Synchroniser FIC (insert/upgrade/close + recluster + persist)
    fic_pairs = fic_sync.sync_provider(provider)

    # 2) Prefilter Lazy : retirer les ids FIC des deux côtés
    left_lf, right_lf = apply_fic_prefilter(
        left_lf, right_lf,
        fic_pairs=fic_pairs,
        left_id=left_id, right_id=right_id
    )

    # 3) Ensuite tu enchaînes avec :
    #    - filtrage des scopes pays (si besoin)
    #    - pipeline exact + fuzzy
    #    - enrich_matches_for_table(...)
    #    - mtbl.merge_write_recluster(provider=provider, current_open_like=..., new_like=...)
    #    (je laisse cette partie intacte puisqu’on l’a déjà écrite ensemble)





