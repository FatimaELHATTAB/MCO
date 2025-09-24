matching_criterias:

  Level1:
    kind: exact
    matching_rule: "3WM Legal Name, Country, LEI"
    mpm: [ "LB_RAISON_SOCIAL_left_cleaned", "CD_PAYS_IMMAT_left", "LEI_final" ]
    provider: [ "TGT_COMPANY_NAME_cleaned", "TGT_COUNTRY", "LEI" ]

  Level2:
    kind: exact
    matching_rule: "2WM Country, ISIN"
    mpm: [ "CD_PAYS_IMMAT_left", "ISIN_final" ]
    provider: [ "TGT_COUNTRY", "ISIN" ]

  Fuzzy:
    kind: fuzzy
    matching_rule: "Fuzzy Legal Name + Country"
    mpm: [ "LB_RAISON_SOCIAL_left_cleaned", "CD_PAYS_IMMAT_left" ]
    provider: [ "TGT_COMPANY_NAME_cleaned", "TGT_COUNTRY" ]
    scorers:
      "LB_RAISON_SOCIAL_left_cleaned": "jw"
      "CD_PAYS_IMMAT_left": "ratio"
    weights:
      "LB_RAISON_SOCIAL_left_cleaned": 0.8
      "CD_PAYS_IMMAT_left": 0.2
    global_min: 92
    block_by_left: "CD_PAYS_IMMAT_left"
    block_by_right: "TGT_COUNTRY"
    top_k: 3


import yaml
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Optional


@dataclass
class MatchingRule:
    name: str
    kind: str  # "exact" or "fuzzy"
    level: str
    mpm_cols: List[str]
    provider_cols: List[str]
    scorers: Optional[Dict[str, str]] = None
    weights: Optional[Dict[str, float]] = None
    global_min: Optional[float] = None
    block_by_left: Optional[str] = None
    block_by_right: Optional[str] = None
    top_k: Optional[int] = None


@dataclass
class RuleSet:
    rules: List[MatchingRule]


def load_rules_for_provider(provider: str, base_dir: str = "config/providers") -> RuleSet:
    path = Path(base_dir) / f"{provider.lower()}.yaml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))

    rules = []
    for level, spec in raw.get("matching_criterias", {}).items():
        rules.append(MatchingRule(
            name=spec.get("matching_rule", level),
            kind=spec.get("kind", "exact"),
            level=level,
            mpm_cols=spec["mpm"],
            provider_cols=spec["provider"],
            scorers=spec.get("scorers"),
            weights=spec.get("weights"),
            global_min=spec.get("global_min"),
            block_by_left=spec.get("block_by_left"),
            block_by_right=spec.get("block_by_right"),
            top_k=spec.get("top_k"),
        ))
    return RuleSet(rules)


import polars as pl
from matching_engine.core.rules_loader import MatchingRule, RuleSet


class ExactMatcherLazy:
    """
    Apply exact matching rules using Polars Lazy.
    """

    def __init__(self, left_id: str, right_id: str):
        self.left_id = left_id
        self.right_id = right_id

    def apply_rule(self, left_lf: pl.LazyFrame, right_lf: pl.LazyFrame, rule: MatchingRule) -> pl.LazyFrame:
        return (
            left_lf.join(
                right_lf,
                left_on=rule.mpm_cols,
                right_on=rule.provider_cols,
                how="inner",
            )
            .select([
                pl.col(self.left_id).alias("left_id"),
                pl.col(self.right_id).alias("right_id"),
                pl.lit(rule.level).alias("level"),
                pl.lit(rule.name).alias("rule_name"),
            ])
        )


import polars as pl
from rapidfuzz import fuzz, distance
from typing import Callable, Dict, List, Optional


SCORER_REGISTRY: Dict[str, Callable[[str, str], float]] = {
    "ratio": fuzz.ratio,
    "partial_ratio": fuzz.partial_ratio,
    "token_sort": fuzz.token_sort_ratio,
    "jw": distance.JaroWinkler().similarity,
}


class RapidFuzzyMatcher:
    """
    Apply fuzzy matching using RapidFuzz.
    """

    def __init__(
        self,
        left_id: str,
        right_id: str,
        columns: List[str],
        peers: Dict[str, str],
        scorers: Dict[str, str],
        weights: Optional[Dict[str, float]] = None,
        global_min: float = 90.0,
        block_by_left: Optional[str] = None,
        block_by_right: Optional[str] = None,
        top_k: Optional[int] = None,
    ):
        self.left_id = left_id
        self.right_id = right_id
        self.columns = columns
        self.peers = peers
        self.scorers = {c: SCORER_REGISTRY[s] for c, s in scorers.items()}
        self.weights = weights or {c: 1.0 for c in columns}
        self.global_min = global_min
        self.block_by_left = block_by_left
        self.block_by_right = block_by_right
        self.top_k = top_k

    def match(self, left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
        results = []

        if self.block_by_left and self.block_by_right:
            block_values = set(left[self.block_by_left].to_list()) & set(right[self.block_by_right].to_list())
            for val in block_values:
                lf_block = left.filter(pl.col(self.block_by_left) == val)
                rf_block = right.filter(pl.col(self.block_by_right) == val)
                results.extend(self._match_block(lf_block, rf_block))
        else:
            results.extend(self._match_block(left, right))

        if not results:
            return pl.DataFrame(schema={"left_id": pl.Utf8, "right_id": pl.Utf8, "score": pl.Float64})

        df = pl.DataFrame(results)

        if self.top_k:
            df = (
                df.sort(["left_id", "score"], descending=[False, True])
                  .group_by("left_id")
                  .head(self.top_k)
            )
        return df

    def _match_block(self, left: pl.DataFrame, right: pl.DataFrame) -> List[Dict[str, object]]:
        matches = []
        for row in left.iter_rows(named=True):
            for prow in right.iter_rows(named=True):
                score_total, wsum = 0.0, 0.0
                for col in self.columns:
                    left_val = str(row[col] or "")
                    right_val = str(prow[self.peers[col]] or "")
                    s = self.scorers[col](left_val, right_val)
                    w = self.weights.get(col, 1.0)
                    score_total += s * w
                    wsum += w
                score = score_total / wsum if wsum > 0 else 0
                if score >= self.global_min:
                    matches.append({
                        "left_id": row[self.left_id],
                        "right_id": prow[self.right_id],
                        "score": score,
                    })
        return matches


import polars as pl
from typing import Optional
from matching_engine.core.rules_loader import RuleSet
from matching_engine.core.exact_matcher_lazy import ExactMatcherLazy
from matching_engine.core.rapidfuzzy_matcher import RapidFuzzyMatcher


class MatchingPipelineLazy:
    """
    Orchestrates exact (sequential) and fuzzy matching.
    """

    def __init__(self, ruleset: RuleSet, left_id: str, right_id: str, run_fuzzy: bool = True):
        self.ruleset = ruleset
        self.left_id = left_id
        self.right_id = right_id
        self.run_fuzzy = run_fuzzy

    def _init_fuzzy_from_rules(self, ruleset: RuleSet) -> Optional[RapidFuzzyMatcher]:
        fuzzy_rules = [r for r in ruleset.rules if r.kind == "fuzzy"]
        if not fuzzy_rules:
            return None
        if len(fuzzy_rules) > 1:
            raise ValueError("Only one fuzzy rule supported for now")

        rule = fuzzy_rules[0]
        return RapidFuzzyMatcher(
            left_id=self.left_id,
            right_id=self.right_id,
            columns=rule.mpm_cols,
            peers=dict(zip(rule.mpm_cols, rule.provider_cols)),
            scorers=rule.scorers,
            weights=rule.weights,
            global_min=rule.global_min or 90.0,
            block_by_left=rule.block_by_left,
            block_by_right=rule.block_by_right,
            top_k=rule.top_k,
        )

    def run(self, left_lf: pl.LazyFrame, right_lf: pl.LazyFrame) -> pl.DataFrame:
        exact_matches = []
        matched_left, matched_right = set(), set()
        matcher = ExactMatcherLazy(self.left_id, self.right_id)

        # sequential exact matching with filtering
        for rule in [r for r in self.ruleset.rules if r.kind == "exact"]:
            lf_left = left_lf.filter(~pl.col(self.left_id).is_in(matched_left))
            lf_right = right_lf.filter(~pl.col(self.right_id).is_in(matched_right))

            df_match = matcher.apply_rule(lf_left, lf_right, rule).collect()
            if not df_match.is_empty():
                exact_matches.append(df_match)
                matched_left.update(df_match["left_id"].to_list())
                matched_right.update(df_match["right_id"].to_list())

        if exact_matches:
            exact_df = pl.concat(exact_matches, how="diagonal_relaxed")
        else:
            exact_df = pl.DataFrame(schema={"left_id": pl.Utf8, "right_id": pl.Utf8, "level": pl.Utf8, "rule_name": pl.Utf8})

        # fuzzy stage
        if not self.run_fuzzy:
            return exact_df

        fuzzy_matcher = self._init_fuzzy_from_rules(self.ruleset)
        if not fuzzy_matcher:
            return exact_df

        left_residual = left_lf.collect().filter(~pl.col(self.left_id).is_in(matched_left))
        right_residual = right_lf.collect().filter(~pl.col(self.right_id).is_in(matched_right))
        fuzzy_df = fuzzy_matcher.match(left_residual, right_residual)

        if fuzzy_df.is_empty():
            return exact_df

        fuzzy_df = fuzzy_df.with_columns([
            pl.lit("Fuzzy").alias("level"),
            pl.lit("Fuzzy Rule").alias("rule_name"),
        ])

        return pl.concat([exact_df, fuzzy_df], how="diagonal_relaxed")




---- usage -----
from matching_engine.core.rules_loader import load_rules_for_provider
from matching_engine.core.pipeline_lazy import MatchingPipelineLazy

# Load rules
ruleset = load_rules_for_provider("BLOOMBERG")

# Load left/right LazyFrames with UnifiedLazyLoader (not shown here)
left_lf, right_lf = ..., ...

# Run pipeline
pipeline = MatchingPipelineLazy(ruleset, left_id="ent_id", right_id="tgt_id", run_fuzzy=True)
matches = pipeline.run(left_lf, right_lf)

print(matches.head())
