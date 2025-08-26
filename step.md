import polars as pl
from typing import List, Tuple, Literal, Iterable

# ---------- helper n-grams (compatible polars "classique") ----------
def _make_ngrams_lf(df: pl.DataFrame, text_col: str, n: int) -> pl.LazyFrame:
    s = pl.col(text_col)
    return (
        df.lazy()
          .with_columns([
              s.cast(pl.Utf8).alias(text_col),
              pl.int_ranges(0, (s.str.len_chars() - (n - 1)).clip(lower_bound=0)).alias("_idxs")
          ])
          .explode("_idxs")
          .with_columns(s.str.slice(pl.col("_idxs"), n).alias("_ngrams"))
          .filter(pl.col("_ngrams").str.len_chars() == n)
          .drop("_idxs")
    )

# ---------- matching sur 1 pays (contains + ngrams) ----------
def _match_one_country_contains(
    rmpm_c: pl.DataFrame,
    provider_c: pl.DataFrame,
    siren_left: str,
    siren_right: str,
    ngram_n: int = 3,
) -> pl.DataFrame:
    # index n-grams
    prov_idx = _make_ngrams_lf(provider_c, siren_right, ngram_n).select(
        [pl.all().exclude("_ngrams"), pl.col("_ngrams").alias("_ng")]
    )
    rmpm_idx = _make_ngrams_lf(rmpm_c, siren_left, ngram_n).select(
        [pl.all().exclude("_ngrams"), pl.col("_ngrams").alias("_ng")]
    )

    # join sur _ng (on est déjà filtré sur le pays)
    candidates = rmpm_idx.join(prov_idx, on="_ng", how="inner").unique()

    # filtre final substring
    out = (
        candidates
        .with_columns(pl.col(siren_right).str.contains(pl.col(siren_left), literal=True).alias("_ok"))
        .filter(pl.col("_ok"))
        .drop(["_ok", "_ng"], strict=False)
        .collect(streaming=True)
    )
    return out

# ---------- wrapper pays-par-pays ----------
def matching_by_country(
    rmpm: pl.DataFrame,
    provider: pl.DataFrame,
    country_left: str,
    country_right: str,
    siren_left: str = "ID_SIREN",
    siren_right: str = "SIREN",
    id_left: str = "ID_INTRN",
    id_right: str = "target_company_EVID",
    matching_rule: str = "CONTAINS: country exact + SIREN substring",
    ngram_n: int = 3,
) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Traite pays par pays pour limiter la mémoire.
    Retourne: (df_merged, rmpm_rest, provider_rest)
    """
    # 0) pré-filtrages utiles
    rmpm_f = (
        rmpm
        .filter(pl.col(country_left).is_not_null())
        .filter(pl.col(siren_left).is_not_null() & (pl.col(siren_left).cast(pl.Utf8).str.len_chars() > 0))
    )
    provider_f = (
        provider
        .filter(pl.col(country_right).is_not_null())
        .filter(pl.col(siren_right).is_not_null() & (pl.col(siren_right).cast(pl.Utf8).str.len_chars() > 0))
    )

    # 1) intersection des pays
    countries_left  = set(rmpm_f.select(pl.col(country_left)).unique().to_series().to_list())
    countries_right = set(provider_f.select(pl.col(country_right)).unique().to_series().to_list())
    common_countries = sorted(countries_left & countries_right)

    results: List[pl.DataFrame] = []
    matched_left_ids: List[pl.Series] = []
    matched_right_ids: List[pl.Series] = []

    # 2) boucle pays
    for c in common_countries:
        rmpm_c = rmpm_f.filter(pl.col(country_left) == c)
        provider_c = provider_f.filter(pl.col(country_right) == c)

        if rmpm_c.height == 0 or provider_c.height == 0:
            continue

        # matching sur ce pays
        m = _match_one_country_contains(rmpm_c, provider_c, siren_left, siren_right, ngram_n=ngram_n)
        if m.height == 0:
            continue

        # ajoute la règle
        m = m.with_columns(pl.lit(matching_rule).alias("Matching Rule"))
        results.append(m)

        # mémorise les IDs matchés (si présents)
        if id_left in m.columns:
            matched_left_ids.append(m[id_left])
        if id_right in m.columns:
            matched_right_ids.append(m[id_right])

        # (optionnel) libère références locales
        del rmpm_c, provider_c, m

    # 3) concat résultats
    if results:
        df_merged = pl.concat(results, how="vertical_relaxed")
        # ordonner/limiter aux colonnes sources + rule
        keep_cols = [c for c in (list(dict.fromkeys(rmpm.columns + provider.columns)) + ["Matching Rule"])
                     if c in df_merged.columns]
        df_merged = df_merged.select(keep_cols)
    else:
        df_merged = pl.DataFrame(schema={})

    # 4) résiduels (on retire les IDs matchés)
    rmpm_rest = rmpm
    provider_rest = provider
    if matched_left_ids:
        left_ids_all = pl.concat(matched_left_ids, how="vertical")
        rmpm_rest = rmpm.filter(~pl.col(id_left).is_in(left_ids_all)) if id_left in rmpm.columns else rmpm
    if matched_right_ids:
        right_ids_all = pl.concat(matched_right_ids, how="vertical")
        provider_rest = provider.filter(~pl.col(id_right).is_in(right_ids_all)) if id_right in provider.columns else provider

    return df_merged, rmpm_rest, provider_rest
