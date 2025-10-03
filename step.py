import polars as pl
from datetime import datetime

def _compare_dataframes_lazy(self,
                             lf_existing: pl.LazyFrame,
                             lf_input: pl.LazyFrame) -> dict:
    """
    Version Lazy de _compare_dataframes.
    - lf_existing : matching table actuelle (ouvertes)
    - lf_input    : nouvelles correspondances calculées
    Retourne un dict de LazyFrames : new_entries, to_close, matched_intern, matched_target, fic, combined
    """

    existing_cols = lf_existing.columns

    # 1) Lignes existantes qui "matchent" l'input
    matched_intern = (
        lf_existing.join(
            lf_input,
            on=["intern_id", "target_name"],
            how="inner"
        )
        .select(existing_cols)
    )

    matched_target = (
        lf_existing.join(
            lf_input,
            on=["target_id", "target_name"],
            how="inner"
        )
        .select(existing_cols)
    )

    # 2) Sous-ensemble FIC (tel qu’on le voit sur la capture)
    fic = lf_existing.filter(pl.col("matching_rule") == "FIC")

    # 3) Union (sans doublons sur tout le schéma existant)
    combined = (
        pl.concat([matched_intern, matched_target, fic], how="vertical_relaxed")
        .unique(subset=existing_cols)
    )

    # 4) Nouvelles entrées à créer : présentes dans l'input mais pas dans combined
    #    (anti-join sur la clé métier ; adapte les colonnes si besoin)
    key_cols = ["intern_id", "target_id", "target_name"]
    new_entries = (
        lf_input.join(combined, on=key_cols, how="anti")
        # ajoute des colonnes si nécessaire (ex: start_date, matching_rule, cluster_id, etc.)
        .with_columns(
            pl.lit(datetime.now()).alias("matching_start_date")
        )
    )

    # 5) Lignes à clôturer : présentes dans combined mais absentes de l'input
    to_close = (
        combined.join(lf_input, on=key_cols, how="anti")
        .with_columns(
            pl.lit(datetime.now()).alias("matching_end_date"),
            pl.lit("NO_LONGER_EXISTS").alias("closure_reason")
        )
    )

    return {
        "matched_intern": matched_intern,
        "matched_target": matched_target,
        "fic": fic,
        "combined": combined,
        "new_entries": new_entries,
        "to_close": to_close,
    }




