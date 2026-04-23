# db_helper.py

import io
import uuid
import pandas as pd


class DbHelper:
    def __init__(self, conn, schema: str, work_schema: str):
        self.conn = conn
        self.schema = schema
        self.work_schema = work_schema

    def execute(self, sql: str):
        with self.conn.cursor() as cur:
            cur.execute(sql)
        self.conn.commit()

    def select_df(self, sql: str) -> pd.DataFrame:
        return pd.read_sql(sql, self.conn)

    def create_work_table_from_df(self, table_name: str, df):
        """
        Crée une table physique de travail + grants + copy bulk.
        Pas une TEMP TABLE, car ta base a besoin de rôles.
        """
        if hasattr(df, "to_pandas"):
            df = df.to_pandas()

        full_table = f"{self.work_schema}.{table_name}"

        self.execute(f"DROP TABLE IF EXISTS {full_table}")

        columns_sql = []
        for col, dtype in zip(df.columns, df.dtypes):
            columns_sql.append(f"{col} {self._map_dtype(dtype)}")

        create_sql = f"""
        CREATE TABLE {full_table} (
            {", ".join(columns_sql)}
        )
        """
        self.execute(create_sql)

        self.grant_work_table_roles(table_name)

        self.copy_df_to_table(table_name, df)

    def copy_df_to_table(self, table_name: str, df):
        if hasattr(df, "to_pandas"):
            df = df.to_pandas()

        full_table = f"{self.work_schema}.{table_name}"
        columns = ", ".join(df.columns)

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, na_rep="\\N")
        buffer.seek(0)

        copy_sql = f"""
        COPY {full_table} ({columns})
        FROM STDIN
        WITH CSV NULL '\\N'
        """

        with self.conn.cursor() as cur:
            cur.copy_expert(copy_sql, buffer)

        self.conn.commit()

    def drop_work_table(self, table_name: str):
        self.execute(f"DROP TABLE IF EXISTS {self.work_schema}.{table_name}")

    def grant_work_table_roles(self, table_name: str):
        full_table = f"{self.work_schema}.{table_name}"

        grants = [
            f"GRANT SELECT ON {full_table} TO role_read",
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON {full_table} TO role_write",
            f"GRANT ALL ON {full_table} TO role_owner",
        ]

        for sql in grants:
            self.execute(sql)

    def _map_dtype(self, dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return "BIGINT"
        if pd.api.types.is_float_dtype(dtype):
            return "DOUBLE PRECISION"
        if pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "TIMESTAMP"
        return "TEXT"

    def generate_work_table_name(self, prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:8]}"




--------------------



# versioning_repository.py


class VersioningRepository:
    def __init__(self, db, schema: str, work_schema: str):
        self.db = db
        self.schema = schema
        self.work_schema = work_schema

    def version_legal_entity(self, df):
        """
        legal_entity:
        - archive dans legal_entity_archive
        - update en place
        - pas de delete
        - pas de close
        """

        work_table = self.db.generate_work_table_name("tmp_legal_entity")
        full_work_table = f"{self.work_schema}.{work_table}"

        self.db.create_work_table_from_df(work_table, df)

        try:
            target_table = "legal_entity"
            archive_table = "legal_entity_archive"
            join_key = "tpn_id"

            target_columns = self._get_table_columns(self.schema, target_table)
            archive_columns = self._get_table_columns(self.schema, archive_table)
            work_columns = self._get_table_columns(self.work_schema, work_table)

            excluded_update_columns = {
                join_key,
                "id",
                "created_at",
                "created_by",
            }

            auto_columns = {
                "updated_at",
                "updated_by",
            }

            archive_base_columns = [
                col for col in target_columns
                if col in archive_columns
            ]

            archive_insert_columns = archive_base_columns.copy()
            archive_select_columns = [f"le.{col}" for col in archive_base_columns]

            if "archived_at" in archive_columns:
                archive_insert_columns.append("archived_at")
                archive_select_columns.append("NOW()")

            if "archived_by" in archive_columns:
                archive_insert_columns.append("archived_by")
                archive_select_columns.append("'UPDATE_IDENTIFICATION'")

            archive_sql = f"""
            INSERT INTO {self.schema}.{archive_table} (
                {", ".join(archive_insert_columns)}
            )
            SELECT
                {", ".join(archive_select_columns)}
            FROM {self.schema}.{target_table} le
            INNER JOIN {full_work_table} src
                ON le.{join_key} = src.{join_key}
            """

            updatable_columns = [
                col for col in target_columns
                if col in work_columns
                and col not in excluded_update_columns
                and col not in auto_columns
            ]

            if not updatable_columns:
                raise ValueError("No updatable columns found for legal_entity")

            set_clauses = [
                f"{col} = src.{col}"
                for col in updatable_columns
            ]

            if "updated_at" in target_columns:
                set_clauses.append("updated_at = NOW()")

            if "updated_by" in target_columns:
                set_clauses.append("updated_by = 'UPDATE_IDENTIFICATION'")

            update_sql = f"""
            UPDATE {self.schema}.{target_table} le
            SET
                {", ".join(set_clauses)}
            FROM {full_work_table} src
            WHERE le.{join_key} = src.{join_key}
            """

            self.db.execute(archive_sql)
            self.db.execute(update_sql)

        finally:
            self.db.drop_work_table(work_table)

    def close_and_insert(self, table_name: str, df, join_columns: list[str]):
        """
        Tables secondaires:
        - close ancienne ligne active
        - insert nouvelle ligne
        """

        work_table = self.db.generate_work_table_name(f"tmp_{table_name}")
        full_work_table = f"{self.work_schema}.{work_table}"

        self.db.create_work_table_from_df(work_table, df)

        try:
            target_columns = self._get_table_columns(self.schema, table_name)
            work_columns = self._get_table_columns(self.work_schema, work_table)

            close_join = " AND ".join([
                f"t.{col} = src.{col}"
                for col in join_columns
            ])

            close_sql = f"""
            UPDATE {self.schema}.{table_name} t
            SET
                closed_at = NOW(),
                business_validity_to = NOW()
            FROM {full_work_table} src
            WHERE {close_join}
              AND t.closed_at IS NULL
            """

            insert_columns = [
                col for col in target_columns
                if col in work_columns
            ]

            insert_select = [
                f"src.{col}"
                for col in insert_columns
            ]

            if "created_at" in target_columns and "created_at" not in insert_columns:
                insert_columns.append("created_at")
                insert_select.append("NOW()")

            if "business_validity_from" in target_columns and "business_validity_from" not in insert_columns:
                insert_columns.append("business_validity_from")
                insert_select.append("NOW()")

            if "business_validity_to" in target_columns and "business_validity_to" not in insert_columns:
                insert_columns.append("business_validity_to")
                insert_select.append("NULL")

            if "closed_at" in target_columns and "closed_at" not in insert_columns:
                insert_columns.append("closed_at")
                insert_select.append("NULL")

            insert_sql = f"""
            INSERT INTO {self.schema}.{table_name} (
                {", ".join(insert_columns)}
            )
            SELECT
                {", ".join(insert_select)}
            FROM {full_work_table} src
            """

            self.db.execute(close_sql)
            self.db.execute(insert_sql)

        finally:
            self.db.drop_work_table(work_table)

    def _get_table_columns(self, schema: str, table_name: str) -> list[str]:
        sql = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        df = self.db.select_df(sql)
        return df["column_name"].to_list()



---------------------


# tpn_insertion.py

from versioning_repository import VersioningRepository


class TpnInsertion:

    def process_immatriculation_update(self, df_updates):
        repo = VersioningRepository(
            db=self.db,
            schema=self.schema,
            work_schema=self.work_schema,
        )

        df_c = df_updates.collect() if hasattr(df_updates, "collect") else df_updates

        df_le = self._df_for_table("legal_entity", df_c)
        repo.version_legal_entity(df_le)

        df_address = self._df_for_table("address", df_c)
        if len(df_address) > 0:
            repo.close_and_insert(
                table_name="address",
                df=df_address,
                join_columns=["tpn_id", "address_type"],
            )

        df_company_identifier = self._df_for_table("company_identifier", df_c)
        if len(df_company_identifier) > 0:
            repo.close_and_insert(
                table_name="company_identifier",
                df=df_company_identifier,
                join_columns=["tpn_id", "company_identifier_nature"],
            )

        df_tax = self._df_for_table("tax", df_c)
        if len(df_tax) > 0:
            repo.close_and_insert(
                table_name="tax",
                df=df_tax,
                join_columns=["tpn_id"],
            )
