import polars as pl
from typing import List, Tuple, Literal, Optional

def _make_ngrams_lf(df: pl.DataFrame, text_col: str, n: int) -> pl.LazyFrame:
    s = pl.col(text_col)
    return (
        df.lazy()
          .with_columns([
              s.cast(pl.Utf8).alias(text_col),
              pl.int_ranges(0, (s.str.len_chars() - (n - 1)).clip_min(0)).alias("_idxs"),
          ])
          .with_columns(pl.col("_idxs").arr.eval(s.str.slice(pl.element(), n)).alias("_ngrams"))
          .drop("_idxs")
          .explode("_ngrams")
          .filter(pl.col("_ngrams").str.len_chars() == n)
    )

def matching(
    rmpm: pl.DataFrame,
    provider: pl.DataFrame,
    key_left_list: List[str],
    key_right_list: List[str],
    matching_rule: str,
    mode: Literal["exact", "contains"] = "exact",
    siren_left: str = "ID_SIREN",
    siren_right: str = "SIREN",
    id_left: str = "ID_INTRN",
    id_right: str = "target_company_EVID",
    batch_size: int = 10_000,
    contains_blocking: Literal["none", "ngrams"] = "ngrams",  # "ngrams" pour scaler
    ngram_n: int = 3,
) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Retourne (df_merged, rmpm_rest, provider_rest)
    - df_merged : paires matchées + colonne "Matching Rule"
    - rmpm_rest / provider_rest : lignes restantes (dédupliquées par id_left/id_right)

    Modes :
      - exact     : join inner sur key_left_list / key_right_list
      - contains  : join exact sur toutes les clés SAUF SIREN, puis filtre substring sur SIREN
                    (avec blocking n-grams par défaut)
    """

    # 0) Copie des DF (on ne mute pas les originaux) + filtrage des null SIREN si "contains"
    rmpm_df = rmpm
    provider_df = provider

    if mode == "contains":
        # on filtre les null / vides côté SIREN
        rmpm_df = rmpm_df.filter(pl.col(siren_left).is_not_null() & (pl.col(siren_left).cast(pl.Utf8).str.len_chars() > 0))
        provider_df = provider_df.filter(pl.col(siren_right).is_not_null() & (pl.col(siren_right).cast(pl.Utf8).str.len_chars() > 0))

    # 1) Jointure selon le mode
    if mode == "exact":
        df_merged = (
            rmpm_df.join(
                provider_df,
                left_on=key_left_list,
                right_on=key_right_list,
                how="inner",
                suffix="_right",
            )
        )

    elif mode == "contains":
        # séparer les clés exactes (toutes sauf les colonnes SIREN)
        pairs = list(zip(key_left_list, key_right_list))
        exact_pairs = [(l, r) for (l, r) in pairs if not (l == siren_left or r == siren_right)]

        if not exact_pairs:
            # pas de clé exacte disponible → on évite le cartésien massif en imposant au moins country si présent
            # sinon on fera un blocking n-grams direct sans join exact
            exact_pairs = []

        if contains_blocking == "none":
            # 1.a) Join exact sur les clés restantes (si on en a)
            if exact_pairs:
                left_keys, right_keys = zip(*exact_pairs)
                candidates = rmpm_df.join(
                    provider_df,
                    left_on=list(left_keys),
                    right_on=list(right_keys),
                    how="inner",
                    suffix="_right",
                )
            else:
                # pas de clés exactes → produit cartésien dangereux; on force un noop join via lit(1)
                # (mieux vaut basculer en ngrams si DF volumineux)
                candidates = rmpm_df.lazy().with_columns(pl.lit(1).alias("_k")).join(
                    provider_df.lazy().with_columns(pl.lit(1).alias("_k")),
                    on="_k",
                    how="inner",
                ).drop("_k").collect(streaming=True)

            # 1.b) Filtre final substring (provider.siren_right contient rmpm.siren_left)
            candidates = candidates if isinstance(candidates, pl.DataFrame) else candidates.collect(streaming=True)
            df_merged = (
                candidates.lazy()
                .with_columns(
                    pl.col(siren_right).str.contains(pl.col(siren_left), literal=True).alias("_ok")
                )
                .filter(pl.col("_ok"))
                .drop("_ok")
                .collect(streaming=True)
            )

        else:
            # contains_blocking == "ngrams"  → scalable
            # On crée un blocking par n-grams sur SIREN, en respectant les clés exactes si présentes.
            # a) index n-grams côté provider (côté "contient")
            prov_idx = _make_ngrams_lf(provider_df, siren_right, ngram_n).select(
                [pl.all().exclude(["_ngrams"]), pl.col("_ngrams").alias("_ng")]
            )
            # b) n-grams côté rmpm (côté "contenu")
            rmpm_idx = _make_ngrams_lf(rmpm_df, siren_left, ngram_n).select(
                [pl.all().exclude(["_ngrams"]), pl.col("_ngrams").alias("_ng")]
            )

            if exact_pairs:
                left_keys, right_keys = zip(*exact_pairs)
                on_pairs = [ (pl.col(l), pl.col(r)) for l, r in zip(left_keys, right_keys) ]
                # join sur clés exactes + n-gram
                candidates = (
                    rmpm_idx.join(
                        prov_idx,
                        on=[pl.col("_ng")] + [pl.col(l) == pl.col(r) for l, r in zip(left_keys, right_keys)],
                        how="inner",
                    )
                    .select(list({*rmpm_df.columns, *[c for c in provider_df.columns]}))  # compact
                    .unique()
                )
            else:
                # join uniquement sur n-gram (plus large)
                candidates = (
                    rmpm_idx.join(prov_idx, on="_ng", how="inner")
                    .select(list({*rmpm_df.columns, *[c for c in provider_df.columns]}))
                    .unique()
                )

            # c) filtre final substring
            df_merged = (
                candidates
                .with_columns(pl.col(siren_right).str.contains(pl.col(siren_left), literal=True).alias("_ok"))
                .filter(pl.col("_ok"))
                .drop("_ok")
                .collect(streaming=True)
            )

    else:
        raise ValueError("mode must be 'exact' or 'contains'")

    # 2) Ajouter la règle
    df_merged = df_merged.with_columns(pl.lit(matching_rule).alias("Matching Rule"))

    # 3) S'assurer que les colonnes 'droite' existent (copie des valeurs gauche si besoin)
    for r_key, l_key in zip(key_right_list, key_left_list):
        if r_key not in df_merged.columns and l_key in df_merged.columns:
            df_merged = df_merged.with_columns(pl.col(l_key).alias(r_key))

    # 4) Réduire aux colonnes souhaitées (garder toutes celles des deux DF + "Matching Rule")
    col_to_keep = list(dict.fromkeys(rmpm.columns + provider.columns + ["Matching Rule"]))  # dédoublonne en gardant l'ordre
    df_merged = df_merged.select([c for c in col_to_keep if c in df_merged.columns])

    # 5) Enlever les lignes matchées des résiduels (par IDs)
    if id_left in df_merged.columns and id_left in rmpm.columns:
        rmpm_rest = rmpm.filter(~pl.col(id_left).is_in(df_merged[id_left]))
    else:
        rmpm_rest = rmpm

    if id_right in df_merged.columns and id_right in provider.columns:
        provider_rest = provider.filter(~pl.col(id_right).is_in(df_merged[id_right]))
    else:
        provider_rest = provider

    return df_merged, rmpm_rest, provider_rest
