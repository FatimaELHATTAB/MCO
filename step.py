# ----- rules -----
rules_for_table_and_sources: |
  SELECT normalized_table, field_name, flux_source, priority, segment_col, segment_val
  FROM priority_rules
  WHERE normalized_table = %(table)s
    AND flux_source = ANY(%(sources)s);

# ----- load normalized (per table) -----
load_normalized_for_keys: |
  WITH keys AS (
    SELECT *
    FROM UNNEST(
      %(flux_sources)s::text[],
      %(local_ids)s::text[]
    ) AS t(flux_source, local_id)
  )
  SELECT
    n.flux_source,
    n.local_id,
    {{COLUMNS}}
  FROM {{TABLE}} n
  JOIN keys k
    ON n.flux_source = k.flux_source
   AND n.local_id    = k.local_id;

# ----- introspection (for update types) -----
get_table_column_types: |
  SELECT column_name, udt_name
  FROM information_schema.columns
  WHERE table_schema = %(schema)s
    AND table_name   = %(table)s
    AND column_name  = ANY(%(columns)s);

# ----- update (no staging) -----
update_from_unnest: |
  UPDATE {{SCHEMA}}.{{TABLE}} t
  SET
    {{SET_CLAUSE}}
  FROM (
    SELECT *
    FROM UNNEST(
      {{UNNEST_ARGS}}
    ) AS u({{UNNEST_COLS}})
  ) s
  WHERE {{WHERE_CLAUSE}};




---------


from __future__ import annotations
from pathlib import Path
import yaml

def load_queries(path: str | Path) -> dict[str, str]:
    with Path(path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}



-----------


from __future__ import annotations
import re

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def safe_ident(name: str) -> str:
    if not _IDENT.match(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return name

def render_table(sql: str, table: str) -> str:
    return sql.replace("{{TABLE}}", safe_ident(table))

def render_columns(sql: str, columns: list[str], alias: str = "n") -> str:
    cols_sql = ",\n    ".join(f'{alias}."{safe_ident(c)}"' for c in columns)
    return sql.replace("{{COLUMNS}}", cols_sql)

def render_update_from_unnest(
    template: str,
    *,
    schema: str,
    table: str,
    pk_cols: list[str],
    update_cols: list[str],
    pg_types: dict[str, str],
) -> str:
    schema = safe_ident(schema)
    table = safe_ident(table)
    pk_cols = [safe_ident(c) for c in pk_cols]
    update_cols = [safe_ident(c) for c in update_cols]
    all_cols = pk_cols + update_cols

    unnest_args_sql = ",\n      ".join(f"%({c})s::{pg_types[c]}[]" for c in all_cols)
    unnest_cols_sql = ", ".join(all_cols)
    set_sql = ",\n    ".join(f'"{c}" = s."{c}"' for c in update_cols)  # NULL bloquant
    where_sql = " AND ".join(f't."{c}" = s."{c}"' for c in pk_cols)

    sql = template
    sql = sql.replace("{{SCHEMA}}", schema)
    sql = sql.replace("{{TABLE}}", table)
    sql = sql.replace("{{UNNEST_ARGS}}", unnest_args_sql)
    sql = sql.replace("{{UNNEST_COLS}}", unnest_cols_sql)
    sql = sql.replace("{{SET_CLAUSE}}", set_sql)
    sql = sql.replace("{{WHERE_CLAUSE}}", where_sql)
    return sql


-------


from __future__ import annotations
from dataclasses import dataclass
from typing import Any
import polars as pl

from query_loader import load_queries
from sql_utils import render_table, render_columns, render_update_from_unnest

@dataclass(frozen=True)
class TableCfg:
    normalized: str
    golden: str
    segment_col: str | None  # ex: 'address_type'
    pk_cols: list[str]       # ex: ['tpn_id'] or ['tpn_id','address_type']

class RulesRepo:
    def __init__(self, db, queries_path: str = "queries.yml") -> None:
        self.db = db
        self.q = load_queries(queries_path)

    def rules(self, table: str, sources: list[str]) -> pl.DataFrame:
        return self.db.read_as_polars(
            self.q["rules_for_table_and_sources"],
            params={"table": table, "sources": sources},
        )

class NormalizedRepo:
    def __init__(self, db, queries_path: str = "queries.yml") -> None:
        self.db = db
        self.q = load_queries(queries_path)

    def load(self, *, table: str, flux_sources: list[str], local_ids: list[str], columns: list[str]) -> pl.DataFrame:
        sql = render_table(self.q["load_normalized_for_keys"], table)
        sql = render_columns(sql, columns, alias="n")
        return self.db.read_as_polars(sql, params={"flux_sources": flux_sources, "local_ids": local_ids})

class Updater:
    def __init__(self, db, *, schema: str = "public", queries_path: str = "queries.yml", chunk_size: int = 50_000) -> None:
        self.db = db
        self.schema = schema
        self.q = load_queries(queries_path)
        self.chunk_size = chunk_size

    def _types(self, table: str, cols: list[str]) -> dict[str, str]:
        df = self.db.read_as_polars(
            self.q["get_table_column_types"],
            params={"schema": self.schema, "table": table, "columns": cols},
        )
        types = {r["column_name"]: r["udt_name"] for r in df.to_dicts()}
        missing = [c for c in cols if c not in types]
        if missing:
            raise ValueError(f"Missing types in {self.schema}.{table}: {missing}")
        return types

    def update(self, *, table: str, df: pl.DataFrame, pk_cols: list[str], update_cols: list[str]) -> None:
        if df.is_empty() or not update_cols:
            return

        cols = pk_cols + update_cols
        pg_types = self._types(table, cols)

        sql = render_update_from_unnest(
            self.q["update_from_unnest"],
            schema=self.schema,
            table=table,
            pk_cols=pk_cols,
            update_cols=update_cols,
            pg_types=pg_types,
        )

        n = df.height
        for i in range(0, n, self.chunk_size):
            chunk = df.slice(i, self.chunk_size)
            params: dict[str, Any] = {c: chunk[c].to_list() for c in cols}
            self.db.execute(sql, params=params)



-------


from __future__ import annotations
import polars as pl

from repos import TableCfg, RulesRepo, NormalizedRepo, Updater

class PrioritizationEngine:
    """
    Table-par-table prioritization + update golden.
    - matching_df: tpn_id, flux_source, local_id
    - priority_rules: (normalized_table, field_name, flux_source, priority, segment_col, segment_val)
    - NULL bloquant
    - no staging: UPDATE ... FROM UNNEST(...)
    """

    def __init__(
        self,
        rules_repo: RulesRepo,
        norm_repo: NormalizedRepo,
        updater: Updater,
        *,
        default_priority: int = 1_000_000,
        keys_chunk_size: int = 200_000,
    ) -> None:
        self.rules_repo = rules_repo
        self.norm_repo = norm_repo
        self.updater = updater
        self.default_priority = default_priority
        self.keys_chunk_size = keys_chunk_size

    def run_table(self, cfg: TableCfg, matching_df: pl.DataFrame) -> pl.DataFrame:
        need = {"tpn_id", "flux_source", "local_id"}
        miss = need - set(matching_df.columns)
        if miss:
            raise ValueError(f"matching_df missing: {sorted(miss)}")

        m = matching_df.select(["tpn_id", "flux_source", "local_id"]).with_columns([
            pl.col("tpn_id").cast(pl.Utf8),
            pl.col("flux_source").cast(pl.Utf8),
            pl.col("local_id").cast(pl.Utf8),
        ])
        sources = sorted(m["flux_source"].unique().to_list())

        rules = self.rules_repo.rules(cfg.normalized, sources)
        if rules.is_empty():
            return pl.DataFrame()

        rules = rules.with_columns([
            pl.col("normalized_table").cast(pl.Utf8),
            pl.col("field_name").cast(pl.Utf8),
            pl.col("flux_source").cast(pl.Utf8),
            pl.col("priority").cast(pl.Int64),
            pl.col("segment_col").cast(pl.Utf8),
            pl.col("segment_val").cast(pl.Utf8),
        ])

        fields = sorted(rules["field_name"].unique().to_list())
        cols = fields[:]  # field_name == colonne normalized
        if cfg.segment_col and cfg.segment_col not in cols:
            cols.append(cfg.segment_col)

        keys = m.select(["flux_source", "local_id"]).unique()
        fs_all = keys["flux_source"].to_list()
        ls_all = keys["local_id"].to_list()

        winners_parts: list[pl.DataFrame] = []

        for i in range(0, len(fs_all), self.keys_chunk_size):
            fs = fs_all[i:i+self.keys_chunk_size]
            ls = ls_all[i:i+self.keys_chunk_size]

            df = self.norm_repo.load(table=cfg.normalized, flux_sources=fs, local_ids=ls, columns=cols)
            if df.is_empty():
                continue

            df = df.with_columns([
                pl.col("flux_source").cast(pl.Utf8),
                pl.col("local_id").cast(pl.Utf8),
            ]).join(m, on=["flux_source","local_id"], how="inner")
            if df.is_empty():
                continue

            id_vars = ["tpn_id", "flux_source", "local_id"]
            if cfg.segment_col:
                id_vars.append(cfg.segment_col)

            long_df = df.melt(
                id_vars=id_vars,
                value_vars=fields,
                variable_name="field_name",
                value_name="value",
            )  # NULL bloquant => no filtering

            # compute segment value from the row (if table is segmented)
            if cfg.segment_col:
                long_df = long_df.with_columns(pl.col(cfg.segment_col).cast(pl.Utf8).alias("seg"))
                # join specific rules on (field_name, flux_source, segment_col, segment_val)
                # rules.segment_col tells *which* column is used; here it must match cfg.segment_col
                rules_t = rules.filter(pl.col("segment_col") == cfg.segment_col)

                specific = long_df.join(
                    rules_t,
                    left_on=["field_name", "flux_source", "seg"],
                    right_on=["field_name", "flux_source", "segment_val"],
                    how="left",
                ).rename({"priority": "prio_specific"})

                general = rules.filter(pl.col("segment_col").is_null() & pl.col("segment_val").is_null()) \
                              .select(["field_name","flux_source","priority"]) \
                              .rename({"priority": "prio_general"})

                ranked = specific.join(general, on=["field_name","flux_source"], how="left") \
                    .with_columns(
                        pl.coalesce(["prio_specific", "prio_general"])
                        .fill_null(self.default_priority)
                        .cast(pl.Int64)
                        .alias("winner_priority")
                    ) \
                    .sort(["tpn_id","field_name","seg","winner_priority","flux_source","local_id"])

                winners = ranked.group_by(["tpn_id","field_name","seg"]).agg([
                    pl.first("value").alias("value"),
                    pl.first("flux_source").alias("winner_source"),
                    pl.first("winner_priority").alias("winner_priority"),
                ]).rename({"seg": cfg.segment_col})

            else:
                # non segmented
                rules_t = rules.filter(pl.col("segment_col").is_null() & pl.col("segment_val").is_null()) \
                              .select(["field_name","flux_source","priority"])

                ranked = long_df.join(rules_t, on=["field_name","flux_source"], how="left") \
                    .with_columns(
                        pl.col("priority").fill_null(self.default_priority).cast(pl.Int64).alias("winner_priority")
                    ) \
                    .sort(["tpn_id","field_name","winner_priority","flux_source","local_id"])

                winners = ranked.group_by(["tpn_id","field_name"]).agg([
                    pl.first("value").alias("value"),
                    pl.first("flux_source").alias("winner_source"),
                    pl.first("winner_priority").alias("winner_priority"),
                ])

            winners_parts.append(winners)

        winners_all = pl.concat(winners_parts, how="vertical") if winners_parts else pl.DataFrame()
        if winners_all.is_empty():
            return winners_all

        # Build update DF (wide) then update golden
        if cfg.segment_col:
            wide = winners_all.pivot(
                index=["tpn_id", cfg.segment_col],
                columns="field_name",
                values="value",
                aggregate_function="first",
            )
            pk_cols = cfg.pk_cols
            update_cols = [c for c in wide.columns if c not in pk_cols]
            self.updater.update(table=cfg.golden, df=wide, pk_cols=pk_cols, update_cols=update_cols)
        else:
            wide = winners_all.pivot(
                index="tpn_id",
                columns="field_name",
                values="value",
                aggregate_function="first",
            )
            pk_cols = cfg.pk_cols
            update_cols = [c for c in wide.columns if c not in pk_cols]
            self.updater.update(table=cfg.golden, df=wide, pk_cols=pk_cols, update_cols=update_cols)

        # audit (long)
        winners_all = winners_all.with_columns(pl.lit(cfg.normalized).alias("normalized_table"))
        return winners_all

    def run_all(self, matching_df: pl.DataFrame, tables: list[TableCfg]) -> pl.DataFrame:
        audits = []
        for cfg in tables:
            w = self.run_table(cfg, matching_df)
            if not w.is_empty():
                audits.append(w)
        return pl.concat(audits, how="vertical") if audits else pl.DataFrame()


------------


import polars as pl
from repos import TableCfg, RulesRepo, NormalizedRepo, Updater
from engine import PrioritizationEngine

TABLES = [
    TableCfg("legal_entity_normalized", "legal_entity", segment_col=None, pk_cols=["tpn_id"]),
    TableCfg("instrument_normalized", "instrument", segment_col=None, pk_cols=["tpn_id"]),
    TableCfg("isin_equity_normalized", "isin_equity", segment_col=None, pk_cols=["tpn_id"]),
    TableCfg("address_normalized", "address", segment_col="address_type", pk_cols=["tpn_id","address_type"]),
    TableCfg("company_identifier_normalized", "company_identifier", segment_col="company_identifier_nature", pk_cols=["tpn_id","company_identifier_nature"]),
]

rules_repo = RulesRepo(db)
norm_repo  = NormalizedRepo(db)
updater    = Updater(db, schema="public", chunk_size=50_000)

engine = PrioritizationEngine(rules_repo, norm_repo, updater, keys_chunk_size=200_000)

audit_df = engine.run_all(matching_df, TABLES)
print(audit_df)
