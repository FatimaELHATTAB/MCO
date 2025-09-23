from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import yaml

@dataclass(frozen=True)
class DataIOConfig:
    table: str
    base_condition: str
    left_id: str
    right_id: str
    distinct: bool
    limit_left: Optional[int]
    limit_right: Optional[int]

def load_data_io_config(path: str | Path) -> DataIOConfig:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    db = raw.get("database") or {}
    ids = raw.get("ids") or {}
    opt = raw.get("options") or {}
    return DataIOConfig(
        table=str(db["table"]),
        base_condition=str(db["base_condition"]),
        left_id=str(ids["left"]),
        right_id=str(ids["right"]),
        distinct=bool(opt.get("distinct", True)),
        limit_left=(int(opt["limit_left"]) if opt.get("limit_left") is not None else None),
        limit_right=(int(opt["limit_right"]) if opt.get("limit_right") is not None else None),
    )



from __future__ import annotations
from typing import Optional, Tuple, Set, List
from dataclasses import dataclass
import polars as pl

from matching_engine.core.rules_loader import RuleSet, MatchingRule, load_rules_for_provider
from matching_engine.db.db_client import DbClient
from .data_config import DataIOConfig


# ---------- helpers colonnes / SQL ----------

def _collect_required_columns(ruleset: RuleSet, include_fuzzy: bool = True) -> Tuple[Set[str], Set[str]]:
    left_cols, right_cols = set(), set()
    for r in ruleset.rules:
        if r.kind == "fuzzy" and not include_fuzzy:
            continue
        left_cols.update(r.mpm_cols)
        right_cols.update(r.provider_cols)
    return left_cols, right_cols

def _qident(s: str) -> str:
    return s if (s.startswith('"') and s.endswith('"')) else f'"{s}"'

def _select_sql(table: str, id_col: str, cols: List[str], where: Optional[str], distinct: bool, limit: Optional[int]) -> str:
    proj = [_qident(id_col)] + [_qident(c) for c in cols if c != id_col]
    sql = f"SELECT {'DISTINCT ' if distinct else ''}{', '.join(proj)} FROM {table}"
    if where:
        sql += f" WHERE {where}"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return sql


# ---------- résultat chargé ----------

@dataclass
class LoadedDataLazy:
    ruleset: RuleSet
    left_lf: pl.LazyFrame
    right_lf: pl.LazyFrame
    left_id: str
    right_id: str


# ---------- loader principal ----------

class UnifiedLazyLoader:
    """
    - Règles: base.yaml + {provider}.yaml (merge)
    - Lecture: une seule table (t_norm_entity) des 2 côtés avec base_condition templatisée
    - Lazy: on convertit le résultat SQL minimal en LazyFrame (.lazy())
    - Cache: on garde le LEFT (RMPM) en mémoire pour les appels suivants si mêmes colonnes
    """

    def __init__(self, db: DbClient, data_cfg: DataIOConfig, providers_cfg_dir: str = "config/providers"):
        self.db = db
        self.cfg = data_cfg
        self.providers_cfg_dir = providers_cfg_dir
        self._left_cache_df: Optional[pl.DataFrame] = None
        self._left_cached_cols: Set[str] = set()

    def load_for_provider(
        self,
        provider: str,
        *,
        base_filename: str = "base.yaml",
        include_fuzzy: bool = True,
        force_reload_left: bool = False,
    ) -> LoadedDataLazy:
        # 1) règles
        ruleset = load_rules_for_provider(provider, base_dir=self.providers_cfg_dir, base_filename=base_filename)

        # 2) colonnes nécessaires
        needed_left, needed_right = _collect_required_columns(ruleset, include_fuzzy=include_fuzzy)

        # 3) LEFT (RMPM) — charge 1 fois + cache; si nouvelles colonnes, on recharge minimal
        left_df = self._load_left_minimal(needed_left, force=force_reload_left)
        left_lf = left_df.lazy()

        # 4) RIGHT (provider courant)
        right_df = self._load_right_minimal(provider, needed_right)
        right_lf = right_df.lazy()

        return LoadedDataLazy(
            ruleset=ruleset,
            left_lf=left_lf,
            right_lf=right_lf,
            left_id=self.cfg.left_id,
            right_id=self.cfg.right_id,
        )

    # ------------- internals -------------

    def _where_for_left(self) -> str:
        # substitute {provider} -> 'RMPM' pour le côté gauche
        return self.cfg.base_condition.format(provider="RMPM")

    def _where_for_right(self, provider: str) -> str:
        return self.cfg.base_condition.format(provider=provider)

    def _load_left_minimal(self, need: Set[str], *, force: bool) -> pl.DataFrame:
        if self._left_cache_df is None or force:
            cols = sorted(need)
            q = _select_sql(
                self.cfg.table, self.cfg.left_id, cols,
                where=self._where_for_left(),
                distinct=self.cfg.distinct, limit=self.cfg.limit_left
            )
            df = self.db.read_as_polars(q, schema=None)
            self._left_cache_df = df
            self._left_cached_cols = set(df.columns)
            return df

        missing = [c for c in need if c not in self._left_cached_cols]
        if missing:
            cols = sorted(self._left_cached_cols | set(missing))
            q = _select_sql(
                self.cfg.table, self.cfg.left_id, cols,
                where=self._where_for_left(),
                distinct=self.cfg.distinct, limit=self.cfg.limit_left
            )
            df = self.db.read_as_polars(q, schema=None)
            self._left_cache_df = df
            self._left_cached_cols = set(df.columns)
        return self._left_cache_df

    def _load_right_minimal(self, provider: str, need: Set[str]) -> pl.DataFrame:
        q = _select_sql(
            self.cfg.table, self.cfg.right_id, sorted(need),
            where=self._where_for_right(provider),
            distinct=self.cfg.distinct, limit=self.cfg.limit_right
        )
        return self.db.read_as_polars(q, schema=None)



from matching_engine.db.db_client import DbClient
from matching_engine.config_yaml import load_yaml_config
from matching_engine.config_bridge import to_env_configs
from matching_engine.io.data_config import load_data_io_config
from matching_engine.io.unified_lazy_loader import UnifiedLazyLoader
from matching_engine.core.pipeline import MatchingPipeline, FuzzyParams

# 0) DB/Vault
app_cfg = load_yaml_config("config/app.yaml")
db_conf, vault_conf = to_env_configs(app_cfg)
db = DbClient(db_conf, vault_conf)

# 1) YAML data factorisé
data_cfg = load_data_io_config("config/data.yaml")

# 2) Lire règles + données (Lazy) pour un provider
loader = UnifiedLazyLoader(db, data_cfg)
loaded = loader.load_for_provider("BLOOMBERG", include_fuzzy=True)

# 3) Pipeline (on `.collect()` seulement à la fin)
pipe = MatchingPipeline(
    loaded.ruleset,
    left_id=loaded.left_id,
    right_id=loaded.right_id,
    run_fuzzy=True,
    fuzzy_params=FuzzyParams(
        columns=["LB_RAISON_SOCIAL_left_cleaned","CD_PAYS_IMMAT_left"],
        peers={"LB_RAISON_SOCIAL_left_cleaned":"TGT_COMPANY_NAME_cleaned",
               "CD_PAYS_IMMAT_left":"TGT_COUNTRY"},
        scorers={"LB_RAISON_SOCIAL_left_cleaned":"jw", "CD_PAYS_IMMAT_left":"ratio"},
        weights={"LB_RAISON_SOCIAL_left_cleaned":0.8, "CD_PAYS_IMMAT_left":0.2},
        per_column_min={"LB_RAISON_SOCIAL_left_cleaned":88, "CD_PAYS_IMMAT_left":95},
        global_min=92,
        combine_mode="weighted",
        block_by_left="CD_PAYS_IMMAT_left",
        block_by_right="TGT_COUNTRY",
        top_k=3,
    ),
)

# La pipeline actuelle prend des DataFrames; comme on est en Lazy, on matérialise ici
left_df  = loaded.left_lf.collect()
right_df = loaded.right_lf.collect()
matches = pipe.run(left_df, right_df)
print(matches.head())

db.close()
