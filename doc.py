# ============================================================
# FILE: duplicate_cluster_processing.sql
# ============================================================

"""
-- ============================================================
-- DUPLICATE CLUSTER PROCESSING
-- MATCHING ARBITRATION QUERIES
-- ============================================================


-- name: get_cluster_arbitration

SELECT
    matching_id,
    tpn_id,
    local_id,
    flux_source,
    cluster_id,
    matching_rule,
    confidence_score,
    country,
    dedup_decision,
    dedup_decision_old,
    created_at
FROM matching
WHERE cluster_id = %(cluster_id)s
  AND flux_source = %(source)s;


-- name: get_cluster_local_ids

SELECT DISTINCT
    local_id
FROM matching
WHERE cluster_id = %(cluster_id)s
  AND flux_source = %(source)s
  AND local_id IS NOT NULL;


-- name: has_validated_matching_by_local_id

SELECT EXISTS (
    SELECT 1
    FROM matching
    WHERE local_id = %(local_id)s
      AND flux_source = %(source)s
      AND dedup_decision IS TRUE
) AS has_validated;


-- name: has_pending_matching_by_local_id

SELECT EXISTS (
    SELECT 1
    FROM matching
    WHERE local_id = %(local_id)s
      AND flux_source = %(source)s
      AND dedup_decision IS NULL
) AS has_pending;


-- name: is_local_id_fully_rejected

SELECT
    COUNT(*) > 0
    AND COUNT(*) FILTER (
        WHERE dedup_decision IS NULL
    ) = 0
    AND COUNT(*) FILTER (
        WHERE dedup_decision IS TRUE
    ) = 0
    AND COUNT(*) FILTER (
        WHERE dedup_decision IS FALSE
    ) = COUNT(*) AS fully_rejected
FROM matching
WHERE local_id = %(local_id)s
  AND flux_source = %(source)s;


-- name: get_validated_identifier

SELECT DISTINCT
    tpn_id,
    local_id,
    flux_source AS source
FROM matching
WHERE local_id = %(local_id)s
  AND flux_source = %(source)s
  AND dedup_decision IS TRUE;


-- name: get_validated_external_matching

SELECT
    matching_id,
    tpn_id,
    local_id,
    flux_source,
    cluster_id,
    matching_rule,
    confidence_score,
    country,
    dedup_decision
FROM matching
WHERE cluster_id = %(cluster_id)s
  AND flux_source = %(source)s
  AND dedup_decision IS TRUE;
"""


# ============================================================
# FILE: duplicate_cluster_processing.py
# ============================================================

import logging
import re
from pathlib import Path

import polars as pl


EXTERNAL_SOURCES = {
    "ORBIS",
    "REFINITIV",
}

INTERNAL_SOURCES = {
    "RMPM",
    "RTX",
}


class DuplicateClusterProcessing:

    def __init__(
        self,
        db,
        tpn_insertion,
        sql_file_path: str,
    ):
        self.db = db
        self.tpn_insertion = tpn_insertion
        self.logger = logging.getLogger(__name__)

        self.queries = self._load_queries(sql_file_path)

    # ========================================================
    # SQL LOADER
    # ========================================================

    @staticmethod
    def _load_queries(sql_file_path: str) -> dict[str, str]:

        sql_content = Path(
            sql_file_path
        ).read_text(
            encoding="utf-8"
        )

        pattern = re.compile(
            r"-- name:\s*(\w+)\s*(.*?)(?=-- name:|\Z)",
            re.DOTALL,
        )

        queries = {}

        for query_name, query_sql in pattern.findall(
            sql_content
        ):
            queries[query_name] = query_sql.strip()

        return queries

    # ========================================================
    # ENTRY POINT
    # ========================================================

    def process(
        self,
        source: str,
        cluster_id: str,
        job_execution_id: int,
    ) -> int:

        self.logger.info(
            "Starting duplicate cluster processing - "
            "source=%s cluster_id=%s",
            source,
            cluster_id,
        )

        if source in EXTERNAL_SOURCES:

            self._process_external_source(
                source=source,
                cluster_id=cluster_id,
                job_execution_id=job_execution_id,
            )

        elif source in INTERNAL_SOURCES:

            self._process_internal_source(
                source=source,
                cluster_id=cluster_id,
                job_execution_id=job_execution_id,
            )

        else:

            raise ValueError(
                f"Unsupported source for MATCHING_ARBITRATION: "
                f"{source}"
            )

        return job_execution_id

    # ========================================================
    # EXTERNAL SOURCES
    # ========================================================

    def _process_external_source(
        self,
        source: str,
        cluster_id: str,
        job_execution_id: int,
    ):

        validated_df = self.db.read_df(
            self.queries[
                "get_validated_external_matching"
            ],
            params={
                "source": source,
                "cluster_id": cluster_id,
            },
        )

        #
        # External source + FALSE only
        #
        # Nothing to do.
        #
        if validated_df.is_empty():

            self.logger.info(
                "No TRUE decision found for external source - "
                "source=%s cluster_id=%s",
                source,
                cluster_id,
            )

            return

        self.logger.info(
            "Validated external arbitration found - "
            "source=%s cluster_id=%s rows=%s",
            source,
            cluster_id,
            validated_df.height,
        )

        self._process_validated_external_matching(
            validated_df=validated_df,
            job_execution_id=job_execution_id,
        )

    def _process_validated_external_matching(
        self,
        validated_df: pl.DataFrame,
        job_execution_id: int,
    ):

        #
        # Reuse normal downstream matching mechanisms.
        #

        vat_pipeline = VatTaxPipeline(
            self.db,
            chunk_size=50_000,
        )

        vat_pipeline.run(
            validated_df,
            tpn_insertion=self.tpn_insertion,
            job_execution_id=job_execution_id,
        )

        self._run_prioritization(
            validated_df
        )

    # ========================================================
    # INTERNAL SOURCES
    # ========================================================

    def _process_internal_source(
        self,
        source: str,
        cluster_id: str,
        job_execution_id: int,
    ):

        local_ids_df = self.db.read_df(
            self.queries[
                "get_cluster_local_ids"
            ],
            params={
                "source": source,
                "cluster_id": cluster_id,
            },
        )

        if local_ids_df.is_empty():

            self.logger.info(
                "No Local ID found for internal arbitration - "
                "source=%s cluster_id=%s",
                source,
                cluster_id,
            )

            return

        local_ids = (
            local_ids_df
            .get_column("local_id")
            .drop_nulls()
            .unique()
            .to_list()
        )

        for local_id in local_ids:

            self._process_internal_local_id(
                source=source,
                local_id=local_id,
                job_execution_id=job_execution_id,
            )

    def _process_internal_local_id(
        self,
        source: str,
        local_id: str,
        job_execution_id: int,
    ):

        #
        # 1. Check whether at least one TRUE exists
        #

        has_validated = self.db.fetch_value(
            self.queries[
                "has_validated_matching_by_local_id"
            ],
            params={
                "source": source,
                "local_id": local_id,
            },
        )

        if has_validated:

            self.logger.info(
                "Validated internal matching found - "
                "source=%s local_id=%s",
                source,
                local_id,
            )

            validated_df = self.db.read_df(
                self.queries[
                    "get_validated_identifier"
                ],
                params={
                    "source": source,
                    "local_id": local_id,
                },
            )

            self._process_validated_internal_matching(
                validated_df=validated_df,
                source=source,
                job_execution_id=job_execution_id,
            )

            return

        #
        # 2. No TRUE.
        #
        # Check whether at least one NULL still exists.
        #

        has_pending = self.db.fetch_value(
            self.queries[
                "has_pending_matching_by_local_id"
            ],
            params={
                "source": source,
                "local_id": local_id,
            },
        )

        if has_pending:

            self.logger.info(
                "Matching arbitration still pending - "
                "source=%s local_id=%s",
                source,
                local_id,
            )

            return

        #
        # 3. No TRUE and no NULL.
        #
        # Check whether all candidates are FALSE.
        #

        fully_rejected = self.db.fetch_value(
            self.queries[
                "is_local_id_fully_rejected"
            ],
            params={
                "source": source,
                "local_id": local_id,
            },
        )

        if not fully_rejected:

            self.logger.info(
                "No action required - "
                "source=%s local_id=%s",
                source,
                local_id,
            )

            return

        #
        # All possible matches for this Local ID
        # have been rejected.
        #

        self.logger.info(
            "All matching candidates rejected - "
            "triggering registration - "
            "source=%s local_id=%s",
            source,
            local_id,
        )

        self._run_registration(
            source=source,
            local_id=local_id,
            job_execution_id=job_execution_id,
        )

    # ========================================================
    # INTERNAL TRUE
    # ========================================================

    def _process_validated_internal_matching(
        self,
        validated_df: pl.DataFrame,
        source: str,
        job_execution_id: int,
    ):

        if validated_df.is_empty():
            return

        identifiers_df = (
            validated_df
            .select(
                "tpn_id",
                "local_id",
                "source",
            )
            .unique()
            .with_columns(
                pl.lit(
                    job_execution_id
                ).alias(
                    "job_execution_id"
                )
            )
        )

        self.logger.info(
            "Inserting validated internal identifiers - "
            "source=%s rows=%s",
            source,
            identifiers_df.height,
        )

        self.tpn_insertion.insert_df(
            identifiers_df,
            table_key="campaign_identifier",
        )

    # ========================================================
    # EXISTING PIPELINES
    # ========================================================

    def _run_prioritization(
        self,
        matching_df: pl.DataFrame,
    ):

        #
        # Reuse your existing prioritization process here.
        #
        # Example:
        #
        # self.pipeline_priorization(matching_df)
        #

        self.logger.info(
            "Running matching prioritization - rows=%s",
            matching_df.height,
        )

        # TODO: replace by existing prioritization method
        pass

    def _run_registration(
        self,
        source: str,
        local_id: str,
        job_execution_id: int,
    ):

        self.logger.info(
            "Running registration - "
            "source=%s local_id=%s",
            source,
            local_id,
        )

        registration_pipeline = RegistrationPipeline(
            self.db
        )

        registration_pipeline.run(
            source=source,
            local_id=local_id,
            job_execution_id=job_execution_id,
        )


# ============================================================
# FILE: task_processor.py
# ============================================================

class TaskProcessor:

    def process_task(
        self,
        task: Task,
    ):

        if task.job_execution_id is None:
            raise ValueError(
                f"La tâche {task.row_id} "
                f"ne contient pas de JobExecutionId."
            )

        job_execution_id = None

        if task.task_type == "UPDATE_IDENTIFICATION":

            self._run_update_identification(
                task
            )

        elif task.task_type == "UPDATE_MATCHING":

            job_execution_id = (
                self._run_matching_pipeline(
                    task
                )
            )

        elif task.task_type == "MATCHING_ARBITRATION":

            duplicate_cluster_processing = (
                DuplicateClusterProcessing(
                    db=self.db,
                    tpn_insertion=self.tpn_insertion,
                    sql_file_path=(
                        "config/"
                        "duplicate_cluster_processing.sql"
                    ),
                )
            )

            duplicate_cluster_processing.process(
                source=task.source,
                cluster_id=task.payload[
                    "cluster_id"
                ],
                job_execution_id=(
                    task.job_execution_id
                ),
            )

            job_execution_id = (
                task.job_execution_id
            )

        return job_execution_id
