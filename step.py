def _lazy_len(self, lf: pl.LazyFrame) -> int:
    return lf.select(pl.len()).collect(streaming=True).item()


def _iter_lazy_batches(
    self,
    lf: pl.LazyFrame,
    batch_size: int = 2_000_000,
):
    total = self._lazy_len(lf)

    for offset in range(0, total, batch_size):
        yield lf.slice(offset, batch_size)


def _explode_if_needed(
    self,
    provider: str,
    df: pl.LazyFrame,
) -> pl.LazyFrame:
    if provider != "ORBIS":
        return df

    return self.data_loader.explode_df(df)


-------------

all_matches = []
all_not_matched = []

for df_normalized_batch in self._iter_lazy_batches(df_normalized, batch_size=2_000_000):

    df_normalized_batch = self._explode_if_needed(
        self.provider,
        df_normalized_batch,
    )

    df_identity_exploded = self._explode_if_needed(
        self.provider,
        df_identity,
    )

    pipeline = MatchingPipelineLazy(
        rule_set,
        left_id="local_id",
        right_id="tpn_id",
        run_fuzzy=False,
    )

    matches, not_matched = pipeline.run(
        df_normalized_batch,
        df_identity_exploded,
    )

    if not matches.is_empty():
        all_matches.append(matches)

    if not not_matched.is_empty():
        all_not_matched.append(not_matched)


------------


if all_matches:
    matches = pl.concat(all_matches, how="diagonal_relaxed").unique()
else:
    matches = pl.DataFrame()

if all_not_matched:
    not_matched = pl.concat(all_not_matched, how="diagonal_relaxed").unique()
else:
    not_matched = pl.DataFrame()

return matches, not_matched, df_identity
