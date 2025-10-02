from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from typing import Optional, Tuple, Set, Dict, List

import polars as pl

from matching_engine.db.db_client import DbClient
from matching_engine.matching_table import MatchingTable


@dataclass(frozen=True)
class FICPairs:
    provider: str
    df: pl.DataFrame  # columns: intern_id (Utf8), target_id (Utf8)


class FICSynchronizer:
    """
    Synchronize FIC pairs between t_ref_fic_client (reference) and t_matching_entity (live table):
      1) Read reference pairs from t_ref_fic_client for a provider
      2) Read current open rows from t_matching_entity (for that provider)
      3) Compute diffs:
           - to_insert (new FIC pairs absent from current)
           - to_upgrade (pairs present but with non-FIC rule)
           - to_close (open FIC pairs not in the reference anymore)
      4) Apply changes (close + insert)
      5) Recompute clusters on ALL open rows for the provider and propagate cluster_id in DB
      6) Return the final active FIC pairs (for prefilter)
    """

    def __init__(self, db: DbClient, t_fic_ref: str = "t_ref_fic_client", t_match: str = "t_matching_entity"):
        self.db = db
        self.t_fic_ref = t_fic_ref
        self.t_match = t_match
        self.today = date.today()

    # ---------- LOADERS ----------

    def load_reference_pairs(self, provider: str) -> pl.DataFrame:
        """
        Read reference pairs from t_ref_fic_client for a provider.
        Expected columns at least: S3DB_id (intern_id), tgt_id (target_id), provider
        """
        q = f"""
        SELECT
          S3DB_id::text AS intern_id,
          tgt_id::text  AS target_id
        FROM {self.t_fic_ref}
        WHERE provider = %s
        """
        conn = self.db._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(q, (provider,))
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
        finally:
            self.db._put(conn)

        if not rows:
            return pl.DataFrame({"intern_id": pl.Series([], dtype=pl.Utf8),
                                 "target_id": pl.Series([], dtype=pl.Utf8)})
        return pl.from_records(rows, columns=cols).with_columns(
            pl.col("intern_id").cast(pl.Utf8),
            pl.col("target_id").cast(pl.Utf8),
        )

    def load_current_open(self, provider: str) -> pl.DataFrame:
        """
        Read ALL open rows for this provider from t_matching_entity (any rule).
        We need them to compute diffs and to recalculate clusters globally.
        """
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
          confidence_score::text AS confidence_score
        FROM {self.t_match}
        WHERE provider = %s
          AND matching_end_date IS NULL
        """
        conn = self.db._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(q, (provider,))
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
        finally:
            self.db._put(conn)
        if not rows:
            return pl.DataFrame({
                "intern_id": pl.Series([], dtype=pl.Utf8),
                "target_id": pl.Series([], dtype=pl.Utf8),
                "target_name": pl.Series([], dtype=pl.Utf8),
                "matching_rule": pl.Series([], dtype=pl.Utf8),
                "matching_start_date": pl.Series([], dtype=pl.Date),
                "cluster_id": pl.Series([], dtype=pl.Utf8),
                "matching_end_date": pl.Series([], dtype=pl.Date),
                "closure_reason": pl.Series([], dtype=pl.Utf8),
                "country": pl.Series([], dtype=pl.Utf8),
                "confidence_score": pl.Series([], dtype=pl.Utf8),
            })
        df = pl.from_records(rows, columns=cols)
        return df.with_columns(
            pl.col("intern_id").cast(pl.Utf8),
            pl.col("target_id").cast(pl.Utf8),
            pl.col("matching_rule").cast(pl.Utf8),
            pl.col("target_name").cast(pl.Utf8),
            pl.col("cluster_id").cast(pl.Utf8),
            pl.col("closure_reason").cast(pl.Utf8),
            pl.col("country").cast(pl.Utf8),
            pl.col("confidence_score").cast(pl.Utf8),
        )

    # ---------- DIFFS ----------

    @staticmethod
    def _pairs_set(df: pl.DataFrame) -> Set[Tuple[str, str]]:
        if df.is_empty():
            return set()
        return set((a, b) for a, b in df.select(["intern_id", "target_id"]).iter_rows())

    def compute_diffs(self, ref_df: pl.DataFrame, current_open: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """
        Returns three DataFrames of pairs (intern_id,target_id):
          - to_insert: pairs in reference but NOT present in current_open at all
          - to_upgrade: pairs in current_open with non-FIC rule that should become FIC
          - to_close: open FIC pairs no longer present in reference
        """
        # current open pairs overall
        cur_pairs = self._pairs_set(current_open)

        # current open FIC pairs
        cur_fic = current_open.filter(pl.col("matching_rule") == "FIC")
        cur_fic_pairs = self._pairs_set(cur_fic)

        # reference pairs
        ref_pairs = self._pairs_set(ref_df)

        # to insert: not present at all
        to_insert_pairs = ref_pairs - cur_pairs

        # to upgrade: present but with non-FIC
        nonfic_pairs = cur_pairs - cur_fic_pairs
        to_upgrade_pairs = ref_pairs & nonfic_pairs

        # to close: FIC open but not in reference
        to_close_pairs = cur_fic_pairs - ref_pairs

        def df_pairs(s: Set[Tuple[str, str]]) -> pl.DataFrame:
            if not s:
                return pl.DataFrame({"intern_id": pl.Series([], dtype=pl.Utf8),
                                     "target_id": pl.Series([], dtype=pl.Utf8)})
            return pl.DataFrame(list(s), schema={"intern_id": pl.Utf8, "target_id": pl.Utf8})

        return df_pairs(to_insert_pairs), df_pairs(to_upgrade_pairs), df_pairs(to_close_pairs)

    # ---------- APPLY CHANGES ----------

    def _insert_fic_rows(self, pairs: pl.DataFrame, provider: str) -> None:
        """Insert FIC rows (minimal payload) with ON CONFLICT DO NOTHING."""
        if pairs.is_empty():
            return
        today = self.today
        rows = pairs.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("intern_name"),
            pl.lit(provider).cast(pl.Utf8).alias("target_name"),
            pl.lit("FIC").cast(pl.Utf8).alias("matching_rule"),
            pl.lit(today).cast(pl.Date).alias("matching_start_date"),
            pl.lit(None).cast(pl.Utf8).alias("cluster_id"),
            pl.lit(None).cast(pl.Date).alias("matching_end_date"),
            pl.lit(None).cast(pl.Utf8).alias("closure_reason"),
            pl.lit(False).cast(pl.Boolean).alias("is_duplicate"),
            pl.lit(None).cast(pl.Boolean).alias("lisbon_decision"),
            pl.lit(None).cast(pl.Date).alias("lisbon_date"),
            pl.lit(None).cast(pl.Utf8).alias("lisbon_user"),
            pl.lit(None).cast(pl.Utf8).alias("confidence_score"),
            pl.lit(None).cast(pl.Utf8).alias("country"),
        ).select([
            "intern_id","target_id","intern_name","target_name","matching_rule",
            "matching_start_date","cluster_id","matching_end_date","closure_reason",
            "is_duplicate","lisbon_decision","lisbon_date","lisbon_user",
            "confidence_score","country"
        ])

        q = f"""
        INSERT INTO {self.t_match} (
          intern_id, target_id, intern_name, target_name,
          matching_rule, matching_start_date, cluster_id,
          matching_end_date, closure_reason, is_duplicate,
          lisbon_decision, lisbon_date, lisbon_user,
          confidence_score, country, provider
        ) VALUES (
          %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        ) ON CONFLICT DO NOTHING
        """
        conn = self.db._conn()
        try:
            with conn.cursor() as cur:
                payload = [tuple(row) + (provider,) for row in rows.iter_rows()]
                cur.executemany(q, payload)
                conn.commit()
        finally:
            self.db._put(conn)

    def _close_pairs(self, pairs: pl.DataFrame, provider: str, reason: str) -> None:
        """Close open rows for these pairs for this provider."""
        if pairs.is_empty():
            return
        q = f"""
        UPDATE {self.t_match}
        SET matching_end_date = %s,
            closure_reason = %s
        WHERE provider = %s
          AND matching_end_date IS NULL
          AND intern_id = %s
          AND target_id = %s
        """
        conn = self.db._conn()
        try:
            with conn.cursor() as cur:
                params = [(self.today, reason, provider, a, b) for a, b in pairs.iter_rows()]
                cur.executemany(q, params)
                conn.commit()
        finally:
            self.db._put(conn)

    def _upgrade_pairs_to_fic(self, pairs: pl.DataFrame, provider: str) -> None:
        """
        For pairs present with non-FIC rule:
          - close old line (reason='FIC_UPGRADE')
          - insert new FIC line
        """
        if pairs.is_empty():
            return
        self._close_pairs(pairs, provider, reason="FIC_UPGRADE")
        self._insert_fic_rows(pairs, provider)

    # ---------- CLUSTERS PROPAGATION ----------

    def _update_cluster_ids_in_db(self, updated: pl.DataFrame, provider: str) -> None:
        """
        Persist cluster_id for active rows (matching_end_date IS NULL) of this provider.
        We update by (provider, intern_id) to keep one pass simple.
        """
        if updated.is_empty():
            return
        # only active rows with cluster_id present
        active = updated.filter(pl.col("matching_end_date").is_null() & pl.col("cluster_id").is_not_null())
        if active.is_empty():
            return

        q = f"""
        UPDATE {self.t_match} AS t
        SET cluster_id = v.cluster_id
        FROM (VALUES %s) AS v(intern_id, cluster_id)
        WHERE t.provider = %s
          AND t.matching_end_date IS NULL
          AND t.intern_id = v.intern_id
        """
        # build VALUES
        values = ",".join(
            ["(%s,%s)"] * active.height
        )
        sql = q.replace("%s", values, 1)  # replace the single %s in VALUES %s

        params: List[tuple] = list(active.select(["intern_id", "cluster_id"]).iter_rows())
        flat: List = []
        for a, b in params:
            flat.extend([a, b])
        flat.append(provider)

        conn = self.db._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, flat)
                conn.commit()
        finally:
            self.db._put(conn)

    # ---------- PUBLIC SYNC ----------

    def sync_provider(self, provider: str, mtbl: MatchingTable) -> FICPairs:
        """
        Execute the full FIC sync for a provider and propagate clusters:
          - compute to_insert, to_upgrade, to_close
          - apply DB changes
          - reload current open rows → recompute clusters → update cluster_id
          - return active FIC pairs (for prefilter)
        """
        # 1) Load sources
        ref = self.load_reference_pairs(provider)           # t_ref_fic_client
        current = self.load_current_open(provider)          # t_matching_entity (open)

        # 2) Diffs
        to_insert, to_upgrade, to_close = self.compute_diffs(ref, current)

        # 3) Apply changes
        if not to_close.is_empty():
            self._close_pairs(to_close, provider, reason="FIC_REMOVED")
        if not to_upgrade.is_empty():
            self._upgrade_pairs_to_fic(to_upgrade, provider)
        if not to_insert.is_empty():
            self._insert_fic_rows(to_insert, provider)

        # 4) Recompute clusters globally for this provider
        updated_active = self.load_current_open(provider)
        # Use MatchingTable's connected-components logic to assign cluster_id
        reclustered = mtbl.recalculate_clusters(updated_active, provider=provider)
        # Persist cluster_id back to DB for all active rows
        self._update_cluster_ids_in_db(reclustered, provider=provider)

        # 5) Return active FIC pairs for prefilter
        active_fic = updated_active.filter(pl.col("matching_rule") == "FIC").select(["intern_id", "target_id"])
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
    """Exclude all entities present in FIC from both sides (by ID), before matching."""
    if fic_pairs.df.is_empty():
        return left_lf, right_lf

    fic_left_ids  = fic_pairs.df.select(pl.col("intern_id").alias(left_id)).unique().lazy()
    fic_right_ids = fic_pairs.df.select(pl.col("target_id").alias(right_id)).unique().lazy()

    left_clean  = left_lf.join(fic_left_ids,  on=left_id,  how="anti")
    right_clean = right_lf.join(fic_right_ids, on=right_id, how="anti")
    return left_clean, right_clean




from matching_engine.fic.fic_sync import FICSynchronizer
from matching_engine.fic.prefilter import apply_fic_prefilter
from matching_engine.matching_table import MatchingTable

def run_provider(...):
    # ... load configs, loader, left_lf/right_lf, ids, ruleset
    mtbl = MatchingTable(db)

    # ---- FIC SYNC (insert/upgrade/close + recluster + return active FIC pairs)
    fic_sync = FICSynchronizer(db)
    fic_pairs = fic_sync.sync_provider(provider, mtbl)

    # ---- exclude FIC from matching
    left_lf, right_lf = apply_fic_prefilter(
        left_lf, right_lf, fic_pairs,
        left_id=left_id, right_id=right_id
    )

    # ... continue with scopes, pipeline, enrich, write to DB ...
