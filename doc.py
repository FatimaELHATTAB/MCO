um_cd/
├─ config/
│  └─ priorisation/
│     └─ vat_tax_queries.yml
├─ prioritisation/
│  ├─ vat_tax_models.py
│  ├─ vat_tax_repository.py
│  ├─ vat_tax_service.py
│  └─ vat_tax_pipeline.py


  -----------------


  load_provider_vat_orbis: |
  WITH keys AS (
      SELECT UNNEST(%(local_ids)s::text[]) AS local_id
  )
  SELECT
      p.local_id::text                                  AS local_id,
      'ORBIS'::text                                     AS flux_source,
      NULLIF(p.tax_data ->> 'tax_id', '')::text         AS tax_id
  FROM own_91109_svg_um.provider_data_normalized p
  JOIN keys k
    ON k.local_id = p.local_id
  WHERE p.flux_source = 'ORBIS'
    AND p.tax_data IS NOT NULL
    AND COALESCE(p.tax_data ->> 'tax_id', '') <> ''

load_provider_vat_refinitiv: |
  WITH keys AS (
      SELECT UNNEST(%(local_ids)s::text[]) AS local_id
  )
  SELECT
      p.local_id::text                                  AS local_id,
      'REFINITIV'::text                                 AS flux_source,
      NULLIF(p.tax_data ->> 'tax_id', '')::text         AS tax_id
  FROM own_91109_svg_um.provider_data_normalized_refinitiv p
  JOIN keys k
    ON k.local_id = p.local_id
  WHERE p.tax_data IS NOT NULL
    AND COALESCE(p.tax_data ->> 'tax_id', '') <> ''

load_existing_active_vat: |
  WITH keys AS (
      SELECT UNNEST(%(tpn_ids)s::text[]) AS tpn_id
  )
  SELECT
      t.id::bigint                      AS id,
      t.tpn_id::text                    AS tpn_id,
      t.tax_id::text                    AS tax_id,
      t.tax_id_status::text             AS tax_id_status,
      t.tax_label::text                 AS tax_label,
      t.business_validity_from          AS business_validity_from,
      t.business_validity_to            AS business_validity_to
  FROM own_91109_svg_um.tax t
  JOIN keys k
    ON k.tpn_id = t.tpn_id
  WHERE t.tax_label = 'VAT'
    AND t.business_validity_to IS NULL

insert_tax_rows: |
  INSERT INTO own_91109_svg_um.tax (
      tpn_id,
      tax_id,
      tax_id_status,
      tax_label,
      business_validity_from,
      business_validity_to,
      created_at,
      created_by,
      closed_at,
      closed_by
  )
  SELECT
      x.tpn_id::text,
      x.tax_id::text,
      x.tax_id_status::text,
      x.tax_label::text,
      x.business_validity_from::date,
      NULL::date,
      NOW(),
      %(created_by)s::text,
      NULL::timestamp,
      NULL::text
  FROM UNNEST(
      %(tpn_ids)s::text[],
      %(tax_ids)s::text[],
      %(tax_id_statuses)s::text[],
      %(tax_labels)s::text[],
      %(business_validity_froms)s::date[]
  ) AS x(tpn_id, tax_id, tax_id_status, tax_label, business_validity_from)

update_tax_rows: |
  UPDATE own_91109_svg_um.tax t
  SET
      tax_id = x.tax_id::text,
      tax_id_status = x.tax_id_status::text,
      tax_label = x.tax_label::text
  FROM UNNEST(
      %(ids)s::bigint[],
      %(tax_ids)s::text[],
      %(tax_id_statuses)s::text[],
      %(tax_labels)s::text[]
  ) AS x(id, tax_id, tax_id_status, tax_label)
  WHERE t.id = x.id





----------------



from dataclasses import dataclass


VAT_LABEL = "VAT"
STATUS_MATCHING_ORBIS = "MATCHING_ORBIS"
STATUS_MATCHING_REFINITIV = "MATCHING_REFINITIV"
STATUS_PROVIDED_BY_RMPM = "PROVIDED_BY_RMPM"
STATUS_CONFIRMED_BY_REFINITIV = "CONFIRMED_BY_REFINITIV"


@dataclass(frozen=True)
class VatCandidate:
    tpn_id: str
    local_id: str
    flux_source: str
    tax_id: str
    tax_label: str
    tax_id_status: str


@dataclass(frozen=True)
class VatDecision:
    action: str   # INSERT / UPDATE / IGNORE
    tpn_id: str
    tax_id: str
    tax_label: str
    tax_id_status: str
    existing_tax_row_id: int | None = None



---------------


from __future__ import annotations

import polars as pl

from load_queries import load_queries


class VatTaxRepository:
    def __init__(
        self,
        db,
        queries_path: str = "config/priorisation/vat_tax_queries.yml",
    ) -> None:
        self.db = db
        self.q = load_queries(queries_path)

    def load_orbis_vat(self, local_ids: list[str]) -> pl.DataFrame:
        if not local_ids:
            return pl.DataFrame(schema={
                "local_id": pl.Utf8,
                "flux_source": pl.Utf8,
                "tax_id": pl.Utf8,
            })
        return self.db.read_as_polars(
            self.q["load_provider_vat_orbis"],
            params={"local_ids": local_ids},
        )

    def load_refinitiv_vat(self, local_ids: list[str]) -> pl.DataFrame:
        if not local_ids:
            return pl.DataFrame(schema={
                "local_id": pl.Utf8,
                "flux_source": pl.Utf8,
                "tax_id": pl.Utf8,
            })
        return self.db.read_as_polars(
            self.q["load_provider_vat_refinitiv"],
            params={"local_ids": local_ids},
        )

    def load_existing_active_vat(self, tpn_ids: list[str]) -> pl.DataFrame:
        if not tpn_ids:
            return pl.DataFrame(schema={
                "id": pl.Int64,
                "tpn_id": pl.Utf8,
                "tax_id": pl.Utf8,
                "tax_id_status": pl.Utf8,
                "tax_label": pl.Utf8,
                "business_validity_from": pl.Date,
                "business_validity_to": pl.Date,
            })
        return self.db.read_as_polars(
            self.q["load_existing_active_vat"],
            params={"tpn_ids": tpn_ids},
        )

    def insert_tax_rows(
        self,
        df: pl.DataFrame,
        *,
        created_by: str = "VAT_ENRICHMENT",
    ) -> None:
        if df.is_empty():
            return

        self.db.execute(
            self.q["insert_tax_rows"],
            params={
                "tpn_ids": df["tpn_id"].to_list(),
                "tax_ids": df["tax_id"].to_list(),
                "tax_id_statuses": df["tax_id_status"].to_list(),
                "tax_labels": df["tax_label"].to_list(),
                "business_validity_froms": df["business_validity_from"].to_list(),
                "created_by": created_by,
            },
        )

    def update_tax_rows(self, df: pl.DataFrame) -> None:
        if df.is_empty():
            return

        self.db.execute(
            self.q["update_tax_rows"],
            params={
                "ids": df["id"].to_list(),
                "tax_ids": df["tax_id"].to_list(),
                "tax_id_statuses": df["tax_id_status"].to_list(),
                "tax_labels": df["tax_label"].to_list(),
            },
        )



---------------


from __future__ import annotations

import logging
from datetime import date

import polars as pl

from prioritisation.vat_tax_models import (
    STATUS_CONFIRMED_BY_REFINITIV,
    STATUS_MATCHING_ORBIS,
    STATUS_MATCHING_REFINITIV,
    STATUS_PROVIDED_BY_RMPM,
    VAT_LABEL,
)


class VatTaxService:
    """
    Règles métier TVA :
    - Refinitiv remplit seulement le vide
    - Orbis remplit le vide
    - Orbis peut écraser MATCHING_REFINITIV / CONFIRMED_BY_REFINITIV
    - PROVIDED_BY_RMPM n'est jamais écrasé
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def build_candidates(
        matching_df: pl.DataFrame,
        provider_vat_df: pl.DataFrame,
    ) -> pl.DataFrame:
        if matching_df.is_empty() or provider_vat_df.is_empty():
            return pl.DataFrame(schema={
                "tpn_id": pl.Utf8,
                "local_id": pl.Utf8,
                "flux_source": pl.Utf8,
                "tax_id": pl.Utf8,
                "tax_label": pl.Utf8,
                "tax_id_status": pl.Utf8,
            })

        joined = (
            matching_df
            .select(["tpn_id", "local_id", "flux_source"])
            .drop_nulls(["tpn_id", "local_id", "flux_source"])
            .unique()
            .join(
                provider_vat_df.select(["local_id", "flux_source", "tax_id"]).unique(),
                on=["local_id", "flux_source"],
                how="inner",
            )
            .filter(pl.col("tax_id").is_not_null() & (pl.col("tax_id") != ""))
        )

        return (
            joined
            .with_columns([
                pl.lit(VAT_LABEL).alias("tax_label"),
                pl.when(pl.col("flux_source") == "ORBIS")
                  .then(pl.lit(STATUS_MATCHING_ORBIS))
                  .when(pl.col("flux_source") == "REFINITIV")
                  .then(pl.lit(STATUS_MATCHING_REFINITIV))
                  .otherwise(pl.lit("UNKNOWN"))
                  .alias("tax_id_status"),
            ])
            .select([
                "tpn_id",
                "local_id",
                "flux_source",
                "tax_id",
                "tax_label",
                "tax_id_status",
            ])
            .unique()
        )

    @staticmethod
    def _empty_insert_df() -> pl.DataFrame:
        return pl.DataFrame(schema={
            "tpn_id": pl.Utf8,
            "tax_id": pl.Utf8,
            "tax_id_status": pl.Utf8,
            "tax_label": pl.Utf8,
            "business_validity_from": pl.Date,
        })

    @staticmethod
    def _empty_update_df() -> pl.DataFrame:
        return pl.DataFrame(schema={
            "id": pl.Int64,
            "tax_id": pl.Utf8,
            "tax_id_status": pl.Utf8,
            "tax_label": pl.Utf8,
        })

    def decide(
        self,
        candidates_df: pl.DataFrame,
        existing_tax_df: pl.DataFrame,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        if candidates_df.is_empty():
            return self._empty_insert_df(), self._empty_update_df()

        existing_by_tpn = (
            existing_tax_df
            .select(["id", "tpn_id", "tax_id", "tax_id_status", "tax_label"])
            .unique(subset=["tpn_id"])
            if not existing_tax_df.is_empty()
            else pl.DataFrame(schema={
                "id": pl.Int64,
                "tpn_id": pl.Utf8,
                "tax_id": pl.Utf8,
                "tax_id_status": pl.Utf8,
                "tax_label": pl.Utf8,
            })
        )

        # priorité de traitement : Refinitiv puis Orbis
        # comme ça Orbis peut écraser ensuite si besoin
        ordered = (
            candidates_df
            .with_columns([
                pl.when(pl.col("flux_source") == "REFINITIV").then(pl.lit(1))
                  .when(pl.col("flux_source") == "ORBIS").then(pl.lit(2))
                  .otherwise(pl.lit(999))
                  .alias("_source_rank")
            ])
            .sort(["tpn_id", "_source_rank"])
        )

        current = {
            row["tpn_id"]: row
            for row in existing_by_tpn.iter_rows(named=True)
        }

        inserts: list[dict] = []
        updates: list[dict] = []

        today = date.today()

        for row in ordered.iter_rows(named=True):
            tpn_id = row["tpn_id"]
            tax_id = row["tax_id"]
            tax_label = row["tax_label"]
            tax_id_status = row["tax_id_status"]
            flux_source = row["flux_source"]

            existing = current.get(tpn_id)

            if existing is None:
                inserts.append({
                    "tpn_id": tpn_id,
                    "tax_id": tax_id,
                    "tax_id_status": tax_id_status,
                    "tax_label": tax_label,
                    "business_validity_from": today,
                })
                current[tpn_id] = {
                    "id": None,
                    "tpn_id": tpn_id,
                    "tax_id": tax_id,
                    "tax_id_status": tax_id_status,
                    "tax_label": tax_label,
                }
                continue

            existing_status = existing["tax_id_status"]
            existing_tax_id = existing["tax_id"]

            # Intouchable
            if existing_status == STATUS_PROVIDED_BY_RMPM:
                continue

            if flux_source == "REFINITIV":
                # Refinitiv remplit seulement le vide
                continue

            if flux_source == "ORBIS":
                # Orbis peut écraser Matching/Confirmed Refinitiv
                if existing_status in {
                    STATUS_MATCHING_REFINITIV,
                    STATUS_CONFIRMED_BY_REFINITIV,
                }:
                    if existing["id"] is not None:
                        updates.append({
                            "id": existing["id"],
                            "tax_id": tax_id,
                            "tax_id_status": tax_id_status,
                            "tax_label": tax_label,
                        })
                    else:
                        # si la ligne venait d'un insert refinitiv du même run,
                        # on remplace l'insert en mémoire
                        inserts = [
                            x for x in inserts
                            if x["tpn_id"] != tpn_id
                        ]
                        inserts.append({
                            "tpn_id": tpn_id,
                            "tax_id": tax_id,
                            "tax_id_status": tax_id_status,
                            "tax_label": tax_label,
                            "business_validity_from": today,
                        })

                    current[tpn_id] = {
                        "id": existing["id"],
                        "tpn_id": tpn_id,
                        "tax_id": tax_id,
                        "tax_id_status": tax_id_status,
                        "tax_label": tax_label,
                    }
                    continue

                # si déjà quelque chose d'autre existe, on ne fait rien
                if existing_tax_id:
                    continue

        insert_df = (
            pl.DataFrame(inserts)
            if inserts
            else self._empty_insert_df()
        )

        update_df = (
            pl.DataFrame(updates)
            if updates
            else self._empty_update_df()
        )

        return insert_df, update_df




-----------------


from __future__ import annotations

import logging

import polars as pl

from prioritisation.vat_tax_repository import VatTaxRepository
from prioritisation.vat_tax_service import VatTaxService


class VatTaxPipeline:
    def __init__(
        self,
        db,
        *,
        chunk_size: int = 50_000,
        queries_path: str = "config/priorisation/vat_tax_queries.yml",
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.repo = VatTaxRepository(db, queries_path=queries_path)
        self.service = VatTaxService()
        self.chunk_size = chunk_size

    def run(self, matching_df: pl.DataFrame) -> None:
        if matching_df.is_empty():
            self.logger.info("VAT pipeline skipped: empty matching_df")
            return

        required = {"tpn_id", "local_id", "flux_source"}
        missing = required - set(matching_df.columns)
        if missing:
            raise ValueError(f"matching_df missing columns: {sorted(missing)}")

        work_df = (
            matching_df
            .select(["tpn_id", "local_id", "flux_source"])
            .drop_nulls(["tpn_id", "local_id", "flux_source"])
            .unique()
            .with_columns([
                pl.col("tpn_id").cast(pl.Utf8),
                pl.col("local_id").cast(pl.Utf8),
                pl.col("flux_source").cast(pl.Utf8),
            ])
            .filter(pl.col("flux_source").is_in(["ORBIS", "REFINITIV"]))
        )

        if work_df.is_empty():
            self.logger.info("VAT pipeline skipped: no ORBIS/REFINITIV rows")
            return

        for chunk in work_df.iter_slices(n_rows=self.chunk_size):
            self._process_chunk(chunk)

    def _process_chunk(self, chunk_df: pl.DataFrame) -> None:
        self.logger.info("VAT pipeline chunk size=%s", chunk_df.height)

        orbis_keys = (
            chunk_df
            .filter(pl.col("flux_source") == "ORBIS")
            .select("local_id")
            .unique()["local_id"]
            .to_list()
        )

        refinitiv_keys = (
            chunk_df
            .filter(pl.col("flux_source") == "REFINITIV")
            .select("local_id")
            .unique()["local_id"]
            .to_list()
        )

        orbis_provider_df = self.repo.load_orbis_vat(orbis_keys)
        refinitiv_provider_df = self.repo.load_refinitiv_vat(refinitiv_keys)

        provider_vat_df = pl.concat(
            [df for df in [orbis_provider_df, refinitiv_provider_df] if not df.is_empty()],
            how="vertical",
        ) if (not orbis_provider_df.is_empty() or not refinitiv_provider_df.is_empty()) else pl.DataFrame(schema={
            "local_id": pl.Utf8,
            "flux_source": pl.Utf8,
            "tax_id": pl.Utf8,
        })

        if provider_vat_df.is_empty():
            self.logger.info("VAT pipeline: no provider VAT found for chunk")
            return

        candidates_df = self.service.build_candidates(chunk_df, provider_vat_df)
        if candidates_df.is_empty():
            self.logger.info("VAT pipeline: no VAT candidates after join")
            return

        existing_tax_df = self.repo.load_existing_active_vat(
            candidates_df.select("tpn_id").unique()["tpn_id"].to_list()
        )

        to_insert_df, to_update_df = self.service.decide(candidates_df, existing_tax_df)

        self.repo.update_tax_rows(to_update_df)
        self.repo.insert_tax_rows(to_insert_df)

        self.logger.info(
            "VAT pipeline done: insert=%s update=%s",
            to_insert_df.height,
            to_update_df.height,
        )


------------



def process_matching(self, matching_output: pl.DataFrame, provider: str, tpn: TpnInsertion, country: str = None):
    if matching_output.is_empty():
        self.logger.info("No Matching founds !!")
        return

    df_formatted = self.format_matching_output(matching_output, provider)

    new_data = recalculate_clusters(df_formatted)
    cluster_stats_df = self.build_cluster_stats_from_df(new_data)

    if not cluster_stats_df.is_empty():
        tpn.insert_df(cluster_stats_df, table_key="cluster_stats")

    tpn.insert_df(new_data, table_key="matching")

    # >>> ENRICHISSEMENT TVA
    vat_pipeline = VatTaxPipeline(self.db, chunk_size=50_000)
    vat_pipeline.run(new_data)

    remaining_data = self.filter_clustered_rows(new_data, cluster_stats_df)
    self.pipeline_priorization(remaining_data)

    self.logger.info("Updating results !!")
    self.logger.info("Matched Done!")





