# ============================================================
# FICHIER : data_loader.py
# ============================================================
# À faire :
# 1. Supprimer l'appel à explode_df() dans load()
# 2. Garder explode_df(), mais elle sera appelée plus tard dans le matching
# ============================================================


def load(
    self,
    provider: str,
    job_execution_id: int,
    country: str,
    params_override=None,
    df_identity=None,
):
    spec = _load_provider_spec(self._config_dir, provider)

    params = dict(spec.params_defaults)

    if params_override:
        params.update(params_override)

    norm_schema = (
        _yaml_to_polars_schema(spec.normalized_schema_yaml)
        if spec.normalized_schema_yaml
        else None
    )

    ident_schema = (
        _yaml_to_polars_schema(spec.identity_schema_yaml)
        if spec.identity_schema_yaml
        else None
    )

    with self._dh_connection() as conn:
        if country is None:
            query = spec.normalized_query.format(
                flux_execution_id=job_execution_id,
                country="IS NULL",
            )
        else:
            query = spec.normalized_query.format(
                flux_execution_id=job_execution_id,
                country=f"= '{country}'",
            )

        df_normalized = _read_query_as_df(
            conn=conn,
            query=query,
            params=params,
            schema=norm_schema,
            chunk_size=spec.chunk_size,
        )

        if df_identity is None:
            df_identity = _read_query_as_df(
                conn=conn,
                query=spec.identity_query,
                params=params,
                schema=ident_schema,
                chunk_size=spec.chunk_size,
            )

    # IMPORTANT :
    # Ne plus faire ça ici :
    #
    # if provider == "ORBIS":
    #     df_normalized = self.explode_df(df_normalized)
    #     df_identity = self.explode_df(df_identity)
    #
    # L'explode sera fait plus tard, batch par batch, dans le matching.

    return df_normalized, df_identity


def explode_df(self, df: pl.LazyFrame) -> pl.LazyFrame:
    df = df.with_columns(
        pl.col("national_registry")
        .str.split("|")
        .alias("national_registry")
    ).explode("national_registry")

    df = df.with_columns(
        pl.col("national_registry_cleaned")
        .str.split("|")
        .alias("national_registry_cleaned")
    ).explode("national_registry_cleaned")

    return df


# ============================================================
# FICHIER : matching_workflow.py
# ============================================================
# À faire :
# 1. Dans la classe MatchingPipelineLazy
# 2. Remplacer l'ancienne fonction run()
# 3. Ajouter _run_single_batch()
# ============================================================


class MatchingPipelineLazy:

    def __init__(self, ruleset, left_id: str, right_id: str, run_fuzzy: bool = False):
        self.ruleset = ruleset
        self.left_id = left_id
        self.right_id = right_id
        self.run_fuzzy = run_fuzzy

    def _init_fuzzy_from_rules(self, rule):
        return RapidFuzzyMatcher(
            block_left=rule.block_by_left,
            block_right=rule.block_by_right,
            top_k=rule.top_k,
        )

    def _run_single_batch(
        self,
        left_lf: pl.LazyFrame,
        right_lf: pl.LazyFrame,
        matched_left: set,
        matched_right: set,
    ) -> tuple[pl.DataFrame, pl.LazyFrame]:

        matches = []

        exact_matcher = ExactMatcherLazy(
            self.left_id,
            self.right_id,
        )

        combined_rules = [
            rule
            for rule in self.ruleset.rules
            if rule.kind in ["exact", "fuzzy"]
        ]

        for rule in combined_rules:
            lf_left = left_lf.filter(
                ~pl.col(self.left_id).is_in(matched_left)
            )

            lf_right = right_lf.filter(
                ~pl.col(self.right_id).is_in(matched_right)
            )

            if rule.kind == "exact":
                print("rule==============", rule.name)

                df_match = exact_matcher.apply_rule(
                    lf_left,
                    lf_right,
                    rule,
                ).collect()

                print("matches exact==============", df_match.shape)

            elif rule.kind == "fuzzy":
                print("rule==============", rule.name)

                fuzzy_matcher = self._init_fuzzy_from_rules(rule)

                fuzzy_df = fuzzy_matcher.match(
                    lf_left.collect(),
                    lf_right.collect(),
                )

                df_match = fuzzy_df.with_columns(
                    pl.lit("Fuzzy").alias("level"),
                    pl.col("country").alias("country"),
                    pl.lit(rule.name).alias("rule_name"),
                    pl.lit(rule.confidence_level).alias("confidence_level"),
                )

                print("matches fuzzy==============", df_match.shape)

            else:
                continue

            if not df_match.is_empty():
                matches.append(df_match)

                matched_left.update(
                    df_match[self.left_id]
                    .drop_nulls()
                    .to_list()
                )

                matched_right.update(
                    df_match[self.right_id]
                    .drop_nulls()
                    .to_list()
                )

        if matches:
            matches_df = pl.concat(
                matches,
                how="diagonal_relaxed",
            )
        else:
            matches_df = pl.DataFrame(
                schema={
                    self.left_id: pl.Utf8,
                    self.right_id: pl.Utf8,
                    "level": pl.Utf8,
                    "rule_name": pl.Utf8,
                    "confidence_level": pl.Utf8,
                }
            )

        left_unmatched = left_lf.filter(
            ~pl.col(self.left_id).is_in(matched_left)
        )

        return matches_df, left_unmatched

    def run(
        self,
        left_lf: pl.LazyFrame,
        right_lf: pl.LazyFrame,
        batch_size: int | None = None,
        left_preprocessor=None,
        right_preprocessor=None,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:

        matched_left = set()
        matched_right = set()

        all_matches = []
        all_not_matched = []

        if right_preprocessor is not None:
            right_lf = right_preprocessor(right_lf)

        if batch_size is None:
            if left_preprocessor is not None:
                left_lf = left_preprocessor(left_lf)

            matches_df, left_unmatched = self._run_single_batch(
                left_lf=left_lf,
                right_lf=right_lf,
                matched_left=matched_left,
                matched_right=matched_right,
            )

            return matches_df, left_unmatched.collect()

        total_rows = (
            left_lf
            .select(pl.len())
            .collect(streaming=True)
            .item()
        )

        for offset in range(0, total_rows, batch_size):
            print(
                f"Running matching batch offset={offset}, "
                f"batch_size={batch_size}"
            )

            left_batch = left_lf.slice(
                offset,
                batch_size,
            )

            if left_preprocessor is not None:
                left_batch = left_preprocessor(left_batch)

            batch_matches, batch_not_matched = self._run_single_batch(
                left_lf=left_batch,
                right_lf=right_lf,
                matched_left=matched_left,
                matched_right=matched_right,
            )

            if not batch_matches.is_empty():
                all_matches.append(batch_matches)

            batch_not_matched_df = batch_not_matched.collect()

            if not batch_not_matched_df.is_empty():
                all_not_matched.append(batch_not_matched_df)

        if all_matches:
            matches_df = (
                pl.concat(
                    all_matches,
                    how="diagonal_relaxed",
                )
                .unique()
            )
        else:
            matches_df = pl.DataFrame()

        if all_not_matched:
            not_matched_df = (
                pl.concat(
                    all_not_matched,
                    how="diagonal_relaxed",
                )
                .unique()
            )
        else:
            not_matched_df = pl.DataFrame()

        return matches_df, not_matched_df


# ============================================================
# FICHIER : matching_run.py OU provider_runner.py
# ============================================================
# À faire :
# Modifier l'appel à pipeline.run()
# ============================================================


def run(self, country, df_identity):
    self.logger.info(
        f"Running Matching for provider {self.provider} - country: {country}"
    )

    rule_set = self.data_loader.get_matching_config(self.provider)
    data_cfg = self.cfg

    df_normalized, df_identity = self.data_loader.load(
        self.provider,
        self.job_execution_id,
        country,
        df_identity=df_identity,
    )

    self.logger.info(f"df_normalized: {df_normalized.collect().shape}")
    self.logger.info(f"df_identity: {df_identity.collect().shape}")

    left_preprocessor = None
    right_preprocessor = None

    if self.provider == "ORBIS":
        left_preprocessor = self.data_loader.explode_df
        right_preprocessor = self.data_loader.explode_df

    pipeline = MatchingPipelineLazy(
        rule_set,
        left_id="local_id",
        right_id="tpn_id",
        run_fuzzy=False,
    )

    matches, not_matched = pipeline.run(
        left_lf=df_normalized,
        right_lf=df_identity,
        batch_size=2_000_000,
        left_preprocessor=left_preprocessor,
        right_preprocessor=right_preprocessor,
    )

    self.logger.info("Matching Done!")

    return matches, not_matched, df_identity
