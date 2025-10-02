# src/matching_engine/fic/fic_io.py
from __future__ import annotations
import polars as pl
from dataclasses import dataclass
from datetime import date
from matching_engine.db.db_client import DbClient

@dataclass(frozen=True)
class FICPairs:
    provider: str
    df: pl.DataFrame  # intern_id, target_id (Utf8)

class FICIO:
    """
    Load and insert FIC pairs into t_matching_entity.
    """

    def __init__(self, db: DbClient, table: str = "t_matching_entity"):
        self.db = db
        self.table = table

    def load_active_pairs(self, provider: str) -> FICPairs:
        """
        Load active FIC pairs for this provider (already stored in DB).
        """
        q = f"""
        SELECT
          intern_id::text AS intern_id,
          target_id::text AS target_id
        FROM {self.table}
        WHERE matching_rule = 'FIC'
          AND matching_end_date IS NULL
          AND target_name = %s
        """
        conn = self.db._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(q, (provider,))
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
        finally:
            self.db._put(conn)

        df = pl.from_records(rows, columns=cols) if rows else pl.DataFrame({"intern_id": [], "target_id": []})
        return FICPairs(provider=provider, df=df)

    def insert_pairs_if_missing(self, provider: str, pairs: pl.DataFrame) -> None:
        """
        Insert FIC pairs into t_matching_entity if not already present.
        """
        if pairs.is_empty():
            return

        today = date.today()

        fic_rows = pairs.with_columns(
            pl.lit(None).alias("intern_name"),
            pl.lit(provider).alias("target_name"),
            pl.lit("FIC").alias("matching_rule"),
            pl.lit(today).cast(pl.Date).alias("matching_start_date"),
            pl.lit(None).alias("cluster_id"),
            pl.lit(None).cast(pl.Date).alias("matching_end_date"),
            pl.lit(None).alias("closure_reason"),
            pl.lit(False).cast(pl.Boolean).alias("is_duplicate"),
            pl.lit(None).cast(pl.Boolean).alias("lisbon_decision"),
            pl.lit(None).cast(pl.Date).alias("lisbon_date"),
            pl.lit(None).alias("lisbon_user"),
            pl.lit(None).alias("confidence_score"),
            pl.lit(None).alias("country"),
        ).select([
            "intern_id","target_id","intern_name","target_name","matching_rule",
            "matching_start_date","cluster_id","matching_end_date","closure_reason",
            "is_duplicate","lisbon_decision","lisbon_date","lisbon_user",
            "confidence_score","country"
        ])

        q = f"""
        INSERT INTO {self.table} (
          intern_id, target_id, intern_name, target_name,
          matching_rule, matching_start_date, cluster_id,
          matching_end_date, closure_reason, is_duplicate,
          lisbon_decision, lisbon_date, lisbon_user,
          confidence_score, country
        ) VALUES (
          %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        ) ON CONFLICT DO NOTHING
        """

        conn = self.db._conn()
        try:
            with conn.cursor() as cur:
                payload = [tuple(fic_rows.row(i)) for i in range(fic_rows.height)]
                cur.executemany(q, payload)
                conn.commit()
        finally:
            self.db._put(conn)




# src/matching_engine/fic/prefilter.py
from __future__ import annotations
import polars as pl
from dataclasses import dataclass

@dataclass(frozen=True)
class FICPairs:
    provider: str
    df: pl.DataFrame  # intern_id, target_id (Utf8)

def apply_fic_prefilter(
    left_lf: pl.LazyFrame,
    right_lf: pl.LazyFrame,
    fic_pairs: FICPairs,
    *,
    left_id: str,
    right_id: str,
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Remove rows already matched by FIC so they don’t enter the matching pipeline.
    """
    if fic_pairs.df.is_empty():
        return left_lf, right_lf

    fic_left_ids  = fic_pairs.df.select(pl.col("intern_id").alias(left_id)).unique().lazy()
    fic_right_ids = fic_pairs.df.select(pl.col("target_id").alias(right_id)).unique().lazy()

    left_clean  = left_lf.join(fic_left_ids,  on=left_id,  how="anti")
    right_clean = right_lf.join(fic_right_ids, on=right_id, how="anti")

    return left_clean, right_clean





fic_io = FICIO(db)
fic_pairs = fic_io.load_active_pairs(provider)

# insérer si manquant
if not fic_pairs.df.is_empty():
    fic_io.insert_pairs_if_missing(provider, fic_pairs.df)

# filtrer avant matching
left_lf, right_lf = apply_fic_prefilter(
    left_lf, right_lf,
    fic_pairs=fic_pairs,
    left_id=left_id, right_id=right_id
)
