from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
import polars as pl

from .rules_loader import RuleSet, MatchingRule
from .exact_adapter import execute_ruleset_exact_in_order
from .rapidfuzzy_matcher import RapidFuzzyMatcher, SCORER_REGISTRY


@dataclass(frozen=True)
class FuzzyParams:
    """
    Paramètres pour exécuter la phase fuzzy.
    - columns: liste des colonnes côté gauche à scorer
    - peers: mapping col_gauche -> col_droite (si noms différents)
    - scorers: mapping col -> nom du scorer dans SCORER_REGISTRY ("jw", "ratio", ...)
    - weights: poids par colonne (pour 'weighted')
    - per_column_min: seuils par colonne
    - global_min: seuil global (0..100)
    - combine_mode: "weighted" | "min"
    - block_by_left/right: colonnes de blocage
    - top_k: nb de candidats à garder par left avant filtrage
    """
    columns: List[str]
    peers: Optional[Dict[str, str]] = None
    scorers: Optional[Dict[str, str]] = None
    weights: Optional[Dict[str, float]] = None
    per_column_min: Optional[Dict[str, float]] = None
    global_min: float = 92.0
    combine_mode: str = "weighted"
    block_by_left: Optional[str] = None
    block_by_right: Optional[str] = None
    top_k: int = 3
    normalize: bool = True


def _anti_join_on_ids(df: pl.DataFrame, ids: pl.DataFrame, id_col: str, ids_col: str = None) -> pl.DataFrame:
    ids_col = ids_col or id_col
    if ids.is_empty():
        return df
    return df.join(ids.select(ids_col).unique(), left_on=[id_col], right_on=[ids_col], how="anti")


def run_matching_pipeline(
    left: pl.DataFrame,
    right: pl.DataFrame,
    ruleset: RuleSet,
    *,
    left_id: str,
    right_id: str,
    run_fuzzy: bool = False,
    fuzzy_params: Optional[FuzzyParams] = None,
    drop_matched_left: bool = True,
    drop_matched_right: bool = False,
) -> pl.DataFrame:
    """
    Orchestrateur global:
      1) Applique les règles exactes dans l'ordre (Level1..).
      2) Si run_fuzzy=True et qu'une règle 'Fuzzy' existe: applique RapidFuzzyMatcher sur les restes.
    Retour: concat des correspondances exactes + fuzzy.
    Colonnes: left_id, right_id, level, rule_name, (score, score_<col>... si fuzzy)
    """
    # 1) Exact
    exact_matches = execute_ruleset_exact_in_order(
        left, right, ruleset, left_id=left_id, right_id=right_id,
        drop_matched_left=drop_matched_left,
        drop_matched_right=drop_matched_right
    )

    if not run_fuzzy:
        return exact_matches

    # 2) Préparer les restes pour fuzzy
    remaining_left = left
    remaining_right = right
    if drop_matched_left and not exact_matches.is_empty():
        remaining_left = _anti_join_on_ids(remaining_left, exact_matches.select("left_id"), id_col=left_id, ids_col="left_id")
    if drop_matched_right and not exact_matches.is_empty():
        remaining_right = _anti_join_on_ids(remaining_right, exact_matches.select("right_id"), id_col=right_id, ids_col="right_id")

    # Si plus rien à matcher, on retourne juste l'exact
    if remaining_left.is_empty() or remaining_right.is_empty():
        return exact_matches

    # 3) Trouver la règle 'Fuzzy' (facultatif mais utile pour renseigner le nom)
    fuzzy_rule: Optional[MatchingRule] = None
    for r in ruleset.rules:
        if r.kind == "fuzzy":
            fuzzy_rule = r
            break

    if not fuzzy_rule:
        # pas de règle fuzzy déclarée: on renvoie juste l'exact
        return exact_matches

    # 4) Construire le matcher fuzzy
    if not fuzzy_params:
        raise ValueError("run_fuzzy=True mais fuzzy_params est None")

    # Construire les fonctions scorers depuis les labels
    if not fuzzy_params.scorers:
        raise ValueError("fuzzy_params.scorers doit être fourni (ex: {'col': 'jw'})")
    scorer_fns = {}
    for col, name in fuzzy_params.scorers.items():
        if name not in SCORER_REGISTRY:
            raise KeyError(f"Scorer inconnu '{name}'. Attendus: {sorted(SCORER_REGISTRY.keys())}")
        scorer_fns[col] = SCORER_REGISTRY[name]

    matcher = RapidFuzzyMatcher(
        columns=fuzzy_params.columns,
        peers=fuzzy_params.peers or {c: c for c in fuzzy_params.columns},
        scorers=scorer_fns,
        weights=fuzzy_params.weights,
        per_column_min=fuzzy_params.per_column_min,
        global_min=fuzzy_params.global_min,
        combine_mode=fuzzy_params.combine_mode,
        block_by_left=fuzzy_params.block_by_left,
        block_by_right=fuzzy_params.block_by_right,
        top_k=fuzzy_params.top_k,
        left_id=left_id,
        right_id=right_id,
        normalize=fuzzy_params.normalize,
    )

    fuzzy_pairs = matcher.match(remaining_left, remaining_right)
    if fuzzy_pairs.is_empty():
        return exact_matches

    # Ajouter métadonnées niveau/nom pour cohérence de sortie
    fuzzy_pairs = fuzzy_pairs.with_columns([
        pl.lit(fuzzy_rule.level).alias("level"),
        pl.lit(fuzzy_rule.name).alias("rule_name"),
    ])[["left_id", "right_id", "level", "rule_name"] + [c for c in fuzzy_pairs.columns if c.startswith("score")] + ["score"]]

    # 5) Concat final (exact en haut, puis fuzzy)
    out_cols = list(set(exact_matches.columns) | set(fuzzy_pairs.columns))
    # Harmoniser colonnes manquantes
    def _ensure_cols(df: pl.DataFrame, cols: List[str]) -> pl.DataFrame:
        missing = [c for c in cols if c not in df.columns]
        if missing:
            df = df.with_columns([pl.lit(None).alias(c) for c in missing])
        return df.select(cols)

    exact_h = _ensure_cols(exact_matches, out_cols)
    fuzzy_h = _ensure_cols(fuzzy_pairs, out_cols)

    out = pl.concat([exact_h, fuzzy_h], how="diagonal_relaxed")
    # Option: trier par level puis left_id
    return out.sort(by=["level", "left_id"])
