@staticmethod
def _filter_matches_by_rule_priority(
    matches: pl.DataFrame,
    rule_set,
) -> pl.DataFrame:

    if matches is None or matches.is_empty():
        return pl.DataFrame()

    # IMPORTANT :
    # l'ordre du ruleset = ordre métier de priorité.
    # On ne se base donc pas uniquement sur confidence_score.
    ordered_rules = [
        rule.name
        for rule in rule_set.rules
        if rule.kind in ("exact", "fuzzy")
    ]

    matched_left = set()
    matched_right = set()

    kept = []

    for rule_name in ordered_rules:

        current = matches.filter(
            pl.col("matching_rule") == rule_name
        )

        if current.is_empty():
            continue

        # Même logique que MatchingPipelineLazy.run():
        # ce qui a déjà matché avec une règle plus prioritaire
        # ne peut plus matcher avec celle-ci.
        current = current.filter(
            ~pl.col("local_id").is_in(matched_left)
            &
            ~pl.col("tpn_id").is_in(matched_right)
        )

        if current.is_empty():
            continue

        kept.append(current)

        matched_left.update(
            current["local_id"]
            .drop_nulls()
            .unique()
            .to_list()
        )

        matched_right.update(
            current["tpn_id"]
            .drop_nulls()
            .unique()
            .to_list()
        )

    if not kept:
        return matches.head(0)

    return pl.concat(
        kept,
        how="diagonal_relaxed",
    )
