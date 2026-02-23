import io
import uuid
from typing import Iterable, List, Optional

import polars as pl
from psycopg2 import sql


class TpnInsertion:
    @staticmethod
    def _iter_chunks(df: pl.DataFrame, chunk_size: int) -> Iterable[pl.DataFrame]:
        for offset in range(0, df.height, chunk_size):
            yield df.slice(offset, chunk_size)

    @staticmethod
    def _parse_schema_table(full_name: str) -> tuple[str, str]:
        if "." not in full_name:
            raise ValueError(f"Expected schema.table, got: {full_name}")
        return tuple(full_name.split(".", 1))  # (schema, table)

    def _get_conflict_cols(self, table_key: str, target_full_name: str) -> List[str]:
        conflict_cols = self.conf["tables"][table_key].get("conflict_cols")
        if not conflict_cols:
            conflict_cols = self.db.get_pk_cols(target_full_name)  # your helper
        if not conflict_cols:
            raise ValueError(
                f"No conflict columns found for {target_full_name}. "
                f"Set conf['tables'][{table_key}]['conflict_cols'] or ensure PK/unique exists."
            )
        return list(conflict_cols)

    def insert_df(
        self,
        df: pl.DataFrame,
        table_key: str,
        *,
        chunk_size: int = 100_000,
        safe: bool = True,
        continue_on_chunk_error: bool = False,
    ) -> None:
        """
        Insert dataframe into target table.

        safe=False:
            COPY directly into target (fastest, fails if any constraint violation).
        safe=True:
            TEMP table + COPY + INSERT ... ON CONFLICT DO NOTHING (robust).
        """
        if df is None or df.height == 0:
            return

        table_name = self.conf["tables"][table_key]["name"]
        target_full_name = f"{self.schema}.{table_name}"
        schema, table = self._parse_schema_table(target_full_name)

        cols = df.columns
        if not cols:
            return

        conn = self.db._conn()

        try:
            total_copied = 0
            chunk_count = 0

            # Prebuild identifiers
            target_ident = sql.Identifier(schema, table)
            cols_ident = [sql.Identifier(c) for c in cols]
            cols_sql = sql.SQL(", ").join(cols_ident)

            if safe:
                # Need conflict cols for ON CONFLICT
                conflict_cols = self._get_conflict_cols(table_key, target_full_name)

                # ON CONFLICT columns must exist in insert col list
                missing = [c for c in conflict_cols if c not in cols]
                if missing:
                    raise ValueError(
                        f"Conflict columns {missing} are missing from dataframe columns. "
                        f"Either include them in df, or change conflict_cols."
                    )

                conflict_sql = sql.SQL(", ").join(sql.Identifier(c) for c in conflict_cols)

                # Create ONE temp table and reuse it per chunk (TRUNCATE)
                tmp_name = f"tmp_{table}_{uuid.uuid4().hex[:10]}"
                tmp_ident = sql.Identifier(tmp_name)

                create_tmp = sql.SQL("""
                    CREATE TEMP TABLE {tmp}
                    (LIKE {target} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING GENERATED)
                    ON COMMIT PRESERVE ROWS
                """).format(tmp=tmp_ident, target=target_ident)

                truncate_tmp = sql.SQL("TRUNCATE {tmp}").format(tmp=tmp_ident)

                copy_tmp = sql.SQL("""
                    COPY {tmp} ({cols})
                    FROM STDIN WITH (FORMAT CSV)
                """).format(tmp=tmp_ident, cols=cols_sql)

                merge = sql.SQL("""
                    INSERT INTO {target} ({cols})
                    SELECT {cols} FROM {tmp}
                    ON CONFLICT ({conflict}) DO NOTHING
                """).format(target=target_ident, cols=cols_sql, tmp=tmp_ident, conflict=conflict_sql)

                with conn.cursor() as cur:
                    cur.execute(create_tmp)
                conn.commit()  # temp table exists for the session anyway; committing here is safe

            else:
                # COPY direct to target
                copy_target = sql.SQL("""
                    COPY {target} ({cols})
                    FROM STDIN WITH (FORMAT CSV)
                """).format(target=target_ident, cols=cols_sql)

            for chunk_idx, chunk in enumerate(self._iter_chunks(df, chunk_size), start=1):
                if chunk.height == 0:
                    continue

                try:
                    with conn.cursor() as cur:
                        if safe:
                            # Clean temp
                            cur.execute(truncate_tmp)

                            # COPY -> temp
                            buf = io.StringIO()
                            chunk.write_csv(buf, include_header=False)
                            buf.seek(0)
                            cur.copy_expert(copy_tmp.as_string(conn), buf)

                            # Merge -> target with ON CONFLICT DO NOTHING
                            cur.execute(merge)

                        else:
                            # COPY direct -> target
                            buf = io.StringIO()
                            chunk.write_csv(buf, include_header=False)
                            buf.seek(0)
                            cur.copy_expert(copy_target.as_string(conn), buf)

                    conn.commit()

                    total_copied += chunk.height
                    chunk_count = chunk_idx
                    mode = "SAFE(temp+conflict)" if safe else "FAST(copy-direct)"
                    self.logger.info(
                        f"[{table_key}] {mode} chunk {chunk_idx} OK "
                        f"({chunk.height} rows processed, total={total_copied}) -> {target_full_name}"
                    )

                except Exception as e:
                    conn.rollback()
                    self.logger.exception(
                        f"[{table_key}] chunk {chunk_idx} FAILED (rows={chunk.height}) -> rollback. Error: {e}"
                    )
                    if not continue_on_chunk_error:
                        raise

            self.logger.info(
                f"[{table_key}] DONE mode={'SAFE' if safe else 'FAST'} "
                f"chunks={chunk_count}, total_processed={total_copied} -> {target_full_name}"
            )

        finally:
            # If safe=True, temp table disappears at session end; we just close.
            conn.close()



-- ============================================================
-- Priority rules table (no historization, no target_table)
--  - field_name: business field to prioritize (e.g. legal_name)
--  - source:     data source (e.g. RCX, OSR, RPMM)
--  - priority:   1 = highest priority
-- ============================================================

CREATE TABLE IF NOT EXISTS priority_rules (
    field_name  VARCHAR(100) NOT NULL,
    source      VARCHAR(50)  NOT NULL,
    priority    INTEGER      NOT NULL CHECK (priority > 0),

    CONSTRAINT pk_priority_rules PRIMARY KEY (field_name, source)
);

-- Avoid ambiguous rankings: for a given field, one priority = one source
CREATE UNIQUE INDEX IF NOT EXISTS uq_priority_rules_field_priority
    ON priority_rules (field_name, priority);

-- Helpful for lookups (optional, but usually useful)
CREATE INDEX IF NOT EXISTS ix_priority_rules_field
    ON priority_rules (field_name);

CREATE INDEX IF NOT EXISTS ix_priority_rules_source
    ON priority_rules (source);

-- ------------------------------------------------------------
-- Example seed (optional) - delete if you don't want seed data
-- ------------------------------------------------------------
-- INSERT INTO priority_rules (field_name, source, priority) VALUES
--   ('legal_name', 'RCX', 1),
--   ('legal_name', 'OSR', 2),
--   ('legal_name', 'RPMM', 3),
--   ('city',       'OSR', 1),
--   ('city',       'RPMM', 2)
-- ON CONFLICT (field_name, source) DO UPDATE
-- SET priority = EXCLUDED.priority;
