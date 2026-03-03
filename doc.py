CREATE TABLE IF NOT EXISTS priority_rules (
  field_name   text    NOT NULL,
  flux_source  text    NOT NULL,
  priority     integer NOT NULL CHECK (priority >= 0),

  is_active    boolean NOT NULL DEFAULT true,
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT priority_rules_pk PRIMARY KEY (field_name, flux_source)
);

CREATE INDEX IF NOT EXISTS priority_rules_flux_source_idx
  ON priority_rules (flux_source);

CREATE INDEX IF NOT EXISTS priority_rules_field_name_idx
  ON priority_rules (field_name);

CREATE INDEX IF NOT EXISTS priority_rules_active_idx
  ON priority_rules (is_active)
  WHERE is_active = true;



------------



INSERT INTO priority_rules (field_name, flux_source, priority)
VALUES
  ('legal_name', 'INSEE', 0),
  ('legal_name', 'RCX',   10),
  ('address',    'INSEE', 0),
  ('address',    'RCX',   20)
ON CONFLICT (field_name, flux_source)
DO UPDATE SET
  priority   = EXCLUDED.priority,
  is_active  = true,
  updated_at = now();



-------


list_fields_for_sources: |
  SELECT DISTINCT field_name
  FROM priority_rules
  WHERE is_active = true
    AND flux_source = ANY(%(sources)s);

load_priority_rules: |
  SELECT field_name, flux_source, priority
  FROM priority_rules
  WHERE is_active = true
    AND field_name  = ANY(%(field_names)s)
    AND flux_source = ANY(%(sources)s);

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
  FROM legal_entity_normalized n
  JOIN keys k
    ON n.flux_source = k.flux_source
   AND n.local_id    = k.local_id;




---------------



from __future__ import annotations
from pathlib import Path
import yaml

def load_queries(path: str | Path) -> dict[str, str]:
    with Path(path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}




----------









from __future__ import annotations
import re

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def safe_col_sql(col: str) -> str:
    """
    Valide un identifiant SQL (colonne) très strictement.
    Important car les identifiants ne peuvent pas être passés en params SQL.
    """
    if not _IDENT.match(col):
        raise ValueError(f"Invalid column name: {col!r}")
    return f'n."{col}"'

def render_columns(sql: str, columns: list[str]) -> str:
    cols_sql = ",\n    ".join(safe_col_sql(c) for c in columns)
    return sql.replace("{{COLUMNS}}", cols_sql)




---------------


from __future__ import annotations
from dataclasses import dataclass
from typing import Any
import polars as pl

@dataclass(frozen=True)
class PrioritizationResult:
    winners: pl.DataFrame
    values_by_cluster: dict[str, dict[str, Any]]
    sources: list[str]
    fields: list[str]



---------------


from __future__ import annotations
from typing import Any
import polars as pl

from query_loader import load_queries
from sql_render import render_columns
from db_protocol import DbClient

class PriorityRepository:
    """
    Accès DB. Le client DB est injecté pour être API-friendly (testable, mockable).
    """

    def __init__(self, db: DbClient, queries_path: str = "prioritization_queries.yml") -> None:
        self.db = db
        self.q = load_queries(queries_path)

    def list_fields_for_sources(self, sources: list[str]) -> list[str]:
        df = self.db.read_as_polars(self.q["list_fields_for_sources"], params={"sources": sources})
        if df.is_empty():
            return []
        return sorted(df["field_name"].unique().to_list())

    def load_priority_rules(self, field_names: list[str], sources: list[str]) -> pl.DataFrame:
        return self.db.read_as_polars(
            self.q["load_priority_rules"],
            params={"field_names": field_names, "sources": sources},
        )

    def load_normalized_for_keys(
        self,
        *,
        flux_sources: list[str],
        local_ids: list[str],
        columns: list[str],
    ) -> pl.DataFrame:
        sql = render_columns(self.q["load_normalized_for_keys"], columns=columns)
        return self.db.read_as_polars(sql, params={"flux_sources": flux_sources, "local_ids": local_ids})

    def load_normalized_for_keys_chunked(
        self,
        *,
        flux_sources: list[str],
        local_ids: list[str],
        columns: list[str],
        chunk_size: int,
    ) -> pl.DataFrame:
        """
        Version chunkée pour gros volumes (évite un UNNEST trop gros).
        """
        if chunk_size <= 0:
            raise ValueError("chunk_size must be > 0")
        if len(flux_sources) != len(local_ids):
            raise ValueError("flux_sources and local_ids must have same length")

        parts: list[pl.DataFrame] = []
        n = len(flux_sources)
        for i in range(0, n, chunk_size):
            part = self.load_normalized_for_keys(
                flux_sources=flux_sources[i : i + chunk_size],
                local_ids=local_ids[i : i + chunk_size],
                columns=columns,
            )
            if not part.is_empty():
                parts.append(part)

        return pl.concat(parts, how="vertical") if parts else pl.DataFrame()



--------------------




from __future__ import annotations
from typing import Any
import polars as pl

from priority_repository import PriorityRepository
from models import PrioritizationResult

class LegalEntityPrioritizer:
    """
    Moteur batch prêt à être mis derrière une API (stateless, dépendances injectées).

    Entrée: matching_df déjà en mémoire (résultat du matching), avec colonnes minimum:
      - cluster_id
      - flux_source
      - local_id
    Optionnel:
      - tpn_id (pour audit)
    """

    def __init__(
        self,
        repo: PriorityRepository,
        *,
        default_priority_when_missing: int = 1_000_000,
        exclude_fields: set[str] | None = None,
        treat_empty_string_as_null: bool = True,
        chunk_size: int | None = 100_000,
    ) -> None:
        self.repo = repo
        self.default_priority = default_priority_when_missing
        self.exclude_fields = exclude_fields or {"flux_source", "local_id", "cluster_id", "tpn_id"}
        self.treat_empty_string_as_null = treat_empty_string_as_null
        self.chunk_size = chunk_size

    def prioritize_all_clusters(self, matching_df: pl.DataFrame) -> PrioritizationResult:
        if matching_df.is_empty():
            return PrioritizationResult(winners=pl.DataFrame(), values_by_cluster={}, sources=[], fields=[])

        required = {"cluster_id", "flux_source", "local_id"}
        missing = required - set(matching_df.columns)
        if missing:
            raise ValueError(f"matching_df missing columns: {sorted(missing)}")

        # Normalize types
        m = matching_df.with_columns([
            pl.col("cluster_id").cast(pl.Utf8),
            pl.col("flux_source").cast(pl.Utf8),
            pl.col("local_id").cast(pl.Utf8),
        ])

        sources = sorted(m["flux_source"].unique().to_list())

        # 1) champs gouvernés
        fields = [f for f in self.repo.list_fields_for_sources(sources) if f not in self.exclude_fields]
        if not fields:
            return PrioritizationResult(winners=pl.DataFrame(), values_by_cluster={}, sources=sources, fields=[])

        # 2) règles
        df_prio = self.repo.load_priority_rules(fields, sources)
        if not df_prio.is_empty():
            df_prio = df_prio.with_columns([
                pl.col("field_name").cast(pl.Utf8),
                pl.col("flux_source").cast(pl.Utf8),
                pl.col("priority").cast(pl.Int64),
            ])

        # 3) charger normalized pour toutes les clés du matching
        keys = m.select(["flux_source", "local_id"]).unique()
        flux_sources = keys["flux_source"].to_list()
        local_ids = keys["local_id"].to_list()

        if self.chunk_size is None:
            df_norm = self.repo.load_normalized_for_keys(
                flux_sources=flux_sources,
                local_ids=local_ids,
                columns=fields,
            )
        else:
            df_norm = self.repo.load_normalized_for_keys_chunked(
                flux_sources=flux_sources,
                local_ids=local_ids,
                columns=fields,
                chunk_size=self.chunk_size,
            )

        if df_norm.is_empty():
            return PrioritizationResult(winners=pl.DataFrame(), values_by_cluster={}, sources=sources, fields=fields)

        # 4) rattacher cluster_id (+ tpn_id si présent)
        join_cols = ["flux_source", "local_id"]
        keep = ["cluster_id"] + (["tpn_id"] if "tpn_id" in m.columns else [])

        df_candidates = (
            df_norm.with_columns([
                pl.col("flux_source").cast(pl.Utf8),
                pl.col("local_id").cast(pl.Utf8),
            ])
            .join(m.select(join_cols + keep), on=join_cols, how="inner")
        )

        if df_candidates.is_empty():
            return PrioritizationResult(winners=pl.DataFrame(), values_by_cluster={}, sources=sources, fields=fields)

        # 5) passage en long
        id_vars = ["cluster_id", "flux_source", "local_id"] + (["tpn_id"] if "tpn_id" in df_candidates.columns else [])
        long_df = df_candidates.melt(
            id_vars=id_vars,
            value_vars=fields,
            variable_name="field_name",
            value_name="value",
        )

        # Règle métier: "NULL bloquant" => on NE filtre PAS les nulls ici.
        # Optionnel: considérer "" comme NULL (souvent souhaité)
        if self.treat_empty_string_as_null and long_df.schema.get("value") == pl.Utf8:
            long_df = long_df.with_columns(
                pl.when(pl.col("value").is_null())
                .then(pl.lit(None))
                .otherwise(pl.col("value").str.strip_chars())
                .alias("value")
            ).with_columns(
                pl.when(pl.col("value") == "").then(pl.lit(None)).otherwise(pl.col("value")).alias("value")
            )

        # 6) join priorités + fallback
        ranked = (
            long_df.join(df_prio, on=["field_name", "flux_source"], how="left")
            .with_columns(pl.col("priority").fill_null(self.default_priority).cast(pl.Int64))
            # tri déterministe (tie-break) — tu peux ajouter updated_at si tu l'as
            .sort(["cluster_id", "field_name", "priority", "flux_source", "local_id"])
        )

        # 7) winner par (cluster, champ) = 1ère source par priorité, valeur telle quelle (même NULL)
        winners = (
            ranked.group_by(["cluster_id", "field_name"])
            .agg([
                pl.first("value").alias("value"),
                pl.first("flux_source").alias("winner_source"),
                pl.first("local_id").alias("winner_local_id"),
                pl.first("priority").alias("winner_priority"),
                *([pl.first("tpn_id").alias("tpn_id")] if "tpn_id" in ranked.columns else []),
            ])
            .sort(["cluster_id", "field_name"])
        )

        # 8) dict par cluster (pratique pour réponse API)
        values_by_cluster: dict[str, dict[str, Any]] = {}
        for cid, sub in winners.group_by("cluster_id"):
            values_by_cluster[str(cid)] = dict(
                zip(sub["field_name"].to_list(), sub["value"].to_list(), strict=True)
            )

        return PrioritizationResult(
            winners=winners,
            values_by_cluster=values_by_cluster,
            sources=sources,
            fields=fields,
        )











-------------------------



import polars as pl
from priority_repository import PriorityRepository
from prioritizer_service import LegalEntityPrioritizer

# matching_df déjà en mémoire (sortie du matching)
matching_df = pl.DataFrame(
    {
        "cluster_id": ["1", "1", "2"],
        "tpn_id": ["TPN10", "TPN10", "TPN99"],
        "flux_source": ["INSEE", "RCX", "RCX"],
        "local_id": ["S1", "77", "88"],
    }
)

repo = PriorityRepository(db, queries_path="prioritization_queries.yml")
svc = LegalEntityPrioritizer(repo, treat_empty_string_as_null=True, chunk_size=100_000)

res = svc.prioritize_all_clusters(matching_df)

print(res.fields)
print(res.winners)
print(res.values_by_cluster["1"])  # dict champ->valeur (NULL possible)
