# historization_repository.py

from typing import List


def chunked(values, size=10_000):
    for i in range(0, len(values), size):
        yield values[i:i + size]


class HistorizationRepository:
    """
    Historisation simple et performante.

    Règles:
    - legal_entity -> archive
    - autres tables -> close (date)
    - PAS de temp table
    - PAS de nouvelle table
    - réutilisable pour UPDATE_MATCHING + DELETE plus tard
    """

    def __init__(self, db, schema: str):
        self.db = db
        self.schema = schema

    # =========================================================
    # PUBLIC
    # =========================================================

    def historize_by_tpn_ids(
        self,
        tpn_ids: List[str],
        source_action: str,
        close_tables: List[str],
        archive_legal_entity: bool = True,
        delete_legal_entity: bool = False,
    ):
        if not tpn_ids:
            return

        if archive_legal_entity:
            self.archive_legal_entity(
                tpn_ids=tpn_ids,
                source_action=source_action,
            )

        for table_name in close_tables:
            self.close_table_by_tpn_id(
                table_name=table_name,
                tpn_ids=tpn_ids,
                source_action=source_action,
            )

        if delete_legal_entity:
            self.delete_legal_entity(tpn_ids)

    # =========================================================
    # LEGAL ENTITY ARCHIVE
    # =========================================================

    def archive_legal_entity(
        self,
        tpn_ids: List[str],
        source_action: str,
    ):
        legal_cols = self._get_table_columns("legal_entity")
        archive_cols = self._get_table_columns("legal_entity_archive")

        common_cols = [
            col
            for col in legal_cols
            if col in archive_cols
        ]

        insert_cols = common_cols.copy()
        select_cols = [f"le.{col}" for col in common_cols]

        if "archived_at" in archive_cols:
            insert_cols.append("archived_at")
            select_cols.append("NOW()")

        if "archived_by" in archive_cols:
            insert_cols.append("archived_by")
            select_cols.append("%s")

        sql = f"""
        INSERT INTO {self.schema}.legal_entity_archive (
            {", ".join(insert_cols)}
        )
        SELECT
            {", ".join(select_cols)}
        FROM {self.schema}.legal_entity le
        WHERE le.tpn_id = ANY(%s)
        """

        for batch in chunked(tpn_ids):
            params = (
                [source_action, batch]
                if "archived_by" in archive_cols
                else [batch]
            )

            self.db.execute(sql, params)

    # =========================================================
    # CLOSE TABLES
    # =========================================================

    def close_table_by_tpn_id(
        self,
        table_name: str,
        tpn_ids: List[str],
        source_action: str,
    ):
        cols = self._get_table_columns(table_name)

        set_clauses = []

        if "closed_at" in cols:
            set_clauses.append("closed_at = NOW()")

        if "business_validity_to" in cols:
            set_clauses.append("business_validity_to = NOW()")

        if "updated_at" in cols:
            set_clauses.append("updated_at = NOW()")

        if "closed_by" in cols:
            set_clauses.append("closed_by = %s")

        if not set_clauses:
            return

        where_clause = "tpn_id = ANY(%s)"

        if "closed_at" in cols:
            where_clause += " AND closed_at IS NULL"

        sql = f"""
        UPDATE {self.schema}.{table_name}
        SET
            {", ".join(set_clauses)}
        WHERE {where_clause}
        """

        for batch in chunked(tpn_ids):
            params = (
                [source_action, batch]
                if "closed_by" in cols
                else [batch]
            )

            self.db.execute(sql, params)

    # =========================================================
    # OPTIONAL DELETE
    # =========================================================

    def delete_legal_entity(self, tpn_ids: List[str]):
        sql = f"""
        DELETE FROM {self.schema}.legal_entity
        WHERE tpn_id = ANY(%s)
        """

        for batch in chunked(tpn_ids):
            self.db.execute(sql, [batch])

    # =========================================================
    # UTILS
    # =========================================================

    def _get_table_columns(self, table_name: str) -> List[str]:
        sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
        """

        df = self.db.select_df(
            sql,
            [self.schema, table_name],
        )

        return df["column_name"].to_list()
