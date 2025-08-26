import polars as pl
from typing import List, Tuple

def _extract_fr_siren_from_siret(df: pl.DataFrame, siret_col: str, out_col: str) -> pl.DataFrame:
    # garde 9 premiers chiffres si possible; sinon null
    return df.with_columns(
        pl.when(pl.col(siret_col).cast(pl.Utf8).str.contains(r"^\d{14}$"))
          .then(pl.col(siret_col).str.slice(0, 9))
          .otherwise(None)
          .alias(out_col)
    )

def _exact_join_country_keys(
    left: pl.DataFrame, right: pl.DataFrame,
    left_keys: List[str], right_keys: List[str]
) -> pl.DataFrame:
    return left.join(right, left_on=left_keys, right_on=right_keys, how="inner", suffix="_right")

def _contains_block_prefix_chunk(
    left: pl.DataFrame, right: pl.DataFrame,
    country_col_left: str, country_col_right: str,
    siren_left: str, siren_right: str,
    prefix_len: int = 5,
    chunk_rows: int = 100_000,
) -> pl.DataFrame:
    """
    Plan B très économe:
      - boucle pays
      - boucle par chunks de `left`
      - blocking par préfixe k (k=5 par défaut)
      - puis str.contains(literal=True)
    """
    # filtre null/vides
    left = left.filter(pl.col(siren_left).is_not_null() & (pl.col(siren_left).cast(pl.Utf8).str.len_chars() > 0))
    right = right.filter(pl.col(siren_right).is_not_null() & (pl.col(siren_right).cast(pl.Utf8).str.len_chars() > 0))

    # pays communs
    c_left  = set(left.select(pl.col(country_col_left)).unique().to_series().to_list())
    c_right = set(right.select(pl.col(country_col_right)).unique().to_series().to_list())
    common  = sorted(c_left & c_right)

    out_parts: List[pl.DataFrame] = []

    for c in common:
        Lc = left.filter(pl.col(country_col_left) == c).with_columns(
            pl.col(siren_left).cast(pl.Utf8).str.slice(0, prefix_len).alias("_pref_L")
        )
        Rc = right.filter(pl.col(country_col_right) == c).with_columns(
            pl.col(siren_right).cast(pl.Utf8).str.slice(0, prefix_len).alias("_pref_R")
        )

        if Lc.height == 0 or Rc.height == 0:
            continue

        # index léger côté right par préfixe pour réduire le join
        Rc_idx = Rc.select([country_col_right, siren_right, "_pref_R"] + [c for c in Rc.columns if c not in {country_col_right, siren_right, "_pref_R"}])

        # chunking du left
        for start in range(0, Lc.height, chunk_rows):
            Lchunk = Lc.slice(start, chunk_rows)
            # join sur (country, prefix)
            cand = (
                Lchunk.join(
                    Rc_idx, left_on=[country_col_left, "_pref_L"], right_on=[country_col_right, "_pref_R"], how="inner"
                )
            )
            if cand.is_empty():
                continue
            # filtre final contains (très peu de paires à ce stade)
            matched = (
                cand.lazy()
                    .with_columns(
                        pl.col(siren_right).str.contains(pl.col(siren_left), literal=True).alias("_ok")
                    )
                    .filter(pl.col("_ok"))
                    .drop("_ok", strict=False)
                    .collect(streaming=True)
            )
            if not matched.is_empty():
                out_parts.append(matched)

    return pl.concat(out_parts, how="vertical_relaxed") if out_parts else pl.DataFrame(schema={})

def matching_contains_memory_safe(
    rmpm: pl.DataFrame,
    provider: pl.DataFrame,
    key_left_country: str = "COUNTRY",
    key_right_country: str = "TGT_COUNTRY",
    siren_left: str = "ID_SIREN_cleaned",
    siret_right: str = "siret_number_cleaned",
    id_left: str = "ID_INTRN",
    id_right: str = "target_company_EVID",
    matching_rule: str = "CONTAINS (memory-safe)",
    prefix_len: int = 5,
    chunk_rows: int = 100_000,
) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    1) Fast-path FR : extrait SIREN=first 9 of SIRET et fait un join exact (country + siren)
    2) Fallback toutes situations restantes : country→chunks→prefix-block→contains
    """
    # ---------- 1) FAST-PATH SIRET→SIREN (évite complètement contains) ----------
    provider_fast = _extract_fr_siren_from_siret(provider, siret_right, out_col="_SIREN_extracted")
    # join exact sur (country, siren)
    fast_matches = _exact_join_country_keys(
        rmpm.filter(pl.col(siren_left).is_not_null()),
        provider_fast.filter(pl.col("_SIREN_extracted").is_not_null()),
        [key_left_country, siren_left],
        [key_right_country, "_SIREN_extracted"],
    )

    # ---------- 2) FALLBACK: pour tout ce qui n'a pas matché ----------
    # retire les IDs déjà matchés (si colonnes présentes)
    if id_left in fast_matches.columns and id_left in rmpm.columns:
        rmpm_rest = rmpm.filter(~pl.col(id_left).is_in(fast_matches[id_left]))
    else:
        rmpm_rest = rmpm

    if id_right in fast_matches.columns and id_right in provider.columns:
        provider_rest = provider.filter(~pl.col(id_right).is_in(fast_matches[id_right]))
    else:
        provider_rest = provider

    # fallback contains (super restreint car beaucoup auront déjà matché via fast-path)
    fallback_matches = _contains_block_prefix_chunk(
        left=rmpm_rest,
        right=provider_rest,
        country_col_left=key_left_country,
        country_col_right=key_right_country,
        siren_left=siren_left,
        siren_right=siret_right,
        prefix_len=prefix_len,
        chunk_rows=chunk_rows,
    )

    # ---------- 3) Concat & shape final ----------
    merged = pl.concat([m for m in [fast_matches, fallback_matches] if not m.is_empty()], how="vertical_relaxed") \
                 if (not fast_matches.is_empty() or not fallback_matches.is_empty()) else pl.DataFrame(schema={})

    if not merged.is_empty():
        merged = merged.with_columns(pl.lit(matching_rule).alias("Matching Rule"))
        keep_cols = [c for c in (list(dict.fromkeys(rmpm.columns + provider.columns)) + ["Matching Rule"]) if c in merged.columns]
        merged = merged.select(keep_cols)

    # recalcul des résiduels définitifs
    r_left = rmpm if id_left not in merged.columns else rmpm.filter(~pl.col(id_left).is_in(merged[id_left]))
    r_right = provider if id_right not in merged.columns else provider.filter(~pl.col(id_right).is_in(merged[id_right]))

    return merged, r_left, r_right


dfm, r_left, r_right = matching_by_country(
    rmpm=rmpm,
    provider=provider,
    country_left="COUNTRY",
    country_right="TGT_COUNTRY",
    siren_left="ID_SIREN_cleaned",
    siren_right="siret_number_cleaned",
    id_left="ID_INTRN",
    id_right="target_company_EVID",
    matching_rule="CONTAINS by country + SIREN substring",
    ngram_n=3,   # monte à 4 si un pays est très gros
)
