import uuid

class VersioningRepository:

    def __init__(self, db, schema):
        self.db = db
        self.schema = schema

    def version_legal_entity(self, df):
        temp = f"tmp_le_{uuid.uuid4().hex[:8]}"
        self.db.create_temp_table_from_df(temp, df)

        target_table = "legal_entity"
        archive_table = "legal_entity_archive"
        join_key = "tpn_id"

        target_columns = self._get_table_columns(target_table)
        archive_columns = self._get_table_columns(archive_table)
        temp_columns = self._get_table_columns(temp)

        excluded_update_columns = {
            join_key,
            "id",
            "created_at",
            "created_by",
        }

        auto_managed_columns = {
            "updated_at",
            "updated_by",
        }

        # archive: on reprend ce qui existe déjà dans legal_entity
        archive_insert_columns = [
            c for c in target_columns if c in archive_columns
        ]
        archive_select_expressions = [
            f"le.{c}" for c in archive_insert_columns
        ]

        if "archived_at" in archive_columns:
            archive_insert_columns.append("archived_at")
            archive_select_expressions.append("NOW()")

        if "archived_by" in archive_columns:
            archive_insert_columns.append("archived_by")
            archive_select_expressions.append("'UPDATE_IDENTIFICATION'")

        archive_cols_sql = ", ".join(archive_insert_columns)
        archive_select_sql = ", ".join(archive_select_expressions)

        # update: seulement colonnes présentes dans la temp
        updatable_columns = [
            c for c in target_columns
            if c in temp_columns
            and c not in excluded_update_columns
            and c not in auto_managed_columns
        ]

        set_clauses = [f"{col} = src.{col}" for col in updatable_columns]

        if "updated_at" in target_columns:
            set_clauses.append("updated_at = NOW()")

        if "updated_by" in target_columns:
            set_clauses.append("updated_by = 'UPDATE_IDENTIFICATION'")

        set_sql = ",\n            ".join(set_clauses)

        archive_sql = f"""
        INSERT INTO {self.schema}.{archive_table} ({archive_cols_sql})
        SELECT {archive_select_sql}
        FROM {self.schema}.{target_table} le
        INNER JOIN {temp} src
            ON le.{join_key} = src.{join_key}
        """

        update_sql = f"""
        UPDATE {self.schema}.{target_table} le
        SET
            {set_sql}
        FROM {temp} src
        WHERE le.{join_key} = src.{join_key}
        """

        self.db.execute(archive_sql)
        self.db.execute(update_sql)
        self.db.execute(f"DROP TABLE IF EXISTS {temp}")

    def _get_table_columns(self, table_name: str) -> list[str]:
        sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{self.schema}'
          AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        df = self.db.select_df(sql)
        return df["column_name"].to_list()
