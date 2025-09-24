import polars as pl
from typing import Optional
from matching_engine.core.rules_loader import RuleSet
from .exact_matcher_lazy import ExactMatcherLazy
from .rapidfuzzy_matcher import RapidFuzzyMatcher


class MatchingPipelineLazy:
    """
    Orchestration :
    1) Applique les règles exactes séquentiellement (filtrage entre étapes)
    2) (Optionnel) applique le fuzzy sur les résiduels
    """

    def __init__(
        self,
        ruleset: RuleSet,
        left_id: str,
        right_id: str,
        run_fuzzy: bool = False,
        fuzzy_matcher: Optional[RapidFuzzyMatcher] = None,
    ):
        self.ruleset = ruleset
        self.left_id = left_id
        self.right_id = right_id
        self.run_fuzzy = run_fuzzy
        self.fuzzy_matcher = fuzzy_matcher

    def run(self, left_lf: pl.LazyFrame, right_lf: pl.LazyFrame) -> pl.DataFrame:
        exact_matches = []
        matched_left, matched_right = set(), set()

        # Phase exact : séquentielle
        for rule in [r for r in self.ruleset.rules if r.kind == "exact"]:
            # filtrer les résiduels avant d'appliquer la règle
            lf_left = left_lf.filter(~pl.col(self.left_id).is_in(matched_left))
            lf_right = right_lf.filter(~pl.col(self.right_id).is_in(matched_right))

            lf_match = ExactMatcherLazy(self.ruleset, self.left_id, self.right_id).apply_rule(lf_left, lf_right, rule)
            df_match = lf_match.collect()

            if not df_match.is_empty():
                exact_matches.append(df_match)
                matched_left.update(df_match["left_id"].to_list())
                matched_right.update(df_match["right_id"].to_list())

        if exact_matches:
            exact_df = pl.concat(exact_matches, how="diagonal_relaxed")
        else:
            exact_df = pl.DataFrame(schema={"left_id": pl.Utf8, "right_id": pl.Utf8, "level": pl.Utf8, "rule_name": pl.Utf8})

        # Phase fuzzy (optionnelle)
        if not self.run_fuzzy or not self.fuzzy_matcher:
            return exact_df

        # Préparer les résiduels pour fuzzy
        left_df = left_lf.collect().filter(~pl.col(self.left_id).is_in(matched_left))
        right_df = right_lf.collect().filter(~pl.col(self.right_id).is_in(matched_right))

        fuzzy_df = self.fuzzy_matcher.match(left_df, right_df)
        if fuzzy_df.is_empty():
            return exact_df

        fuzzy_df = fuzzy_df.with_columns([
            pl.lit("Fuzzy").alias("level"),
            pl.lit("Fuzzy Legal Name").alias("rule_name"),
        ])

        return pl.concat([exact_df, fuzzy_df], how="diagonal_relaxed")
