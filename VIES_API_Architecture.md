# ============================================================
# MATCHING STATISTICS NOTEBOOK
# ============================================================

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 200)


# ============================================================
# 1. CONFIGURATION
# ============================================================

# Une seule source analysée à la fois
SOURCE = "ORBIS"                      # ex : "ORBIS", "REFINITIV"
SOURCE_COL = "flux_source"

# Colonnes principales
TPN_ID_COL = "tpn_id"
CLUSTER_COL = "cluster_id"

# À adapter au vrai nom de ta colonne
# ex : "matching_code" ou "matching_rule"
MATCHING_CODE_COL = "matching_code"

# Pays du TPN
TPN_COUNTRY_COL = "incorporation_country"

# Pays provenant de la source
SOURCE_COUNTRY_COL = "country"

# Si tu veux analyser seulement les lignes ouvertes
ONLY_OPEN_ROWS = False
CLOSED_AT_COL = "closed_at"


# ============================================================
# 2. CHARGEMENT DES DONNÉES
# ============================================================

# Si ton dataframe existe déjà et s'appelle df,
# tu peux commenter cette ligne.

df = pd.read_csv("matching.csv")

# Exemple SQL à utiliser à la place si besoin :
#
# from sqlalchemy import create_engine
#
# engine = create_engine(
#     "postgresql+psycopg2://USER:PASSWORD@HOST:PORT/DATABASE"
# )
#
# query = """
# SELECT
#     matching_id,
#     tpn_id,
#     local_id,
#     flux_source,
#     cluster_id,
#     matching_rule,
#     confidence_score,
#     country,
#     incorporation_country,
#     created_at,
#     updated_at,
#     closed_at
# FROM own_91109_svg_um.matching
# """
#
# df = pd.read_sql(query, engine)


# ============================================================
# 3. CONTRÔLE DES COLONNES
# ============================================================

required_cols = [
    SOURCE_COL,
    TPN_ID_COL,
    CLUSTER_COL,
    MATCHING_CODE_COL,
    TPN_COUNTRY_COL,
    SOURCE_COUNTRY_COL
]

if ONLY_OPEN_ROWS:
    required_cols.append(CLOSED_AT_COL)

missing_cols = [
    col for col in required_cols
    if col not in df.columns
]

if missing_cols:
    raise KeyError(
        f"Colonnes manquantes : {missing_cols}"
    )


# ============================================================
# 4. FILTRAGE SUR UNE SOURCE
# ============================================================

work = df.copy()

# Optionnel : uniquement les lignes ouvertes
if ONLY_OPEN_ROWS:
    work = work[
        work[CLOSED_AT_COL].isna()
    ].copy()

# Une seule source
work = work[
    work[SOURCE_COL]
    .astype(str)
    .str.upper()
    .eq(SOURCE.upper())
].copy()

# Suppression des lignes sans matching code
work = work[
    work[MATCHING_CODE_COL].notna()
].copy()


print("=" * 70)
print(f"SOURCE ANALYSÉE : {SOURCE}")
print("=" * 70)

print(
    f"Nombre de lignes : "
    f"{len(work):,}"
)

print(
    f"Nombre de TPN distincts : "
    f"{work[TPN_ID_COL].nunique():,}"
)

print(
    f"Nombre de clusters distincts : "
    f"{work[CLUSTER_COL].nunique():,}"
)

print(
    f"Nombre de matching codes distincts : "
    f"{work[MATCHING_CODE_COL].nunique():,}"
)


# ============================================================
# 5. STAT 1
# NOMBRE DE MATCHINGS PAR MATCHING CODE
# ============================================================

stat_matching_code = (
    work
    .groupby(
        MATCHING_CODE_COL,
        dropna=False
    )
    .size()
    .rename("number_of_matchings")
    .reset_index()
    .sort_values(
        "number_of_matchings",
        ascending=False
    )
)

print("\n")
print("=" * 70)
print("STAT 1 - NOMBRE DE MATCHINGS PAR MATCHING CODE")
print("=" * 70)

display(stat_matching_code)


plt.figure(figsize=(12, 6))

plt.bar(
    stat_matching_code[MATCHING_CODE_COL].astype(str),
    stat_matching_code["number_of_matchings"]
)

plt.title(
    f"Nombre de matchings par matching code - {SOURCE}"
)

plt.xlabel("Matching code")
plt.ylabel("Nombre de matchings")

plt.xticks(
    rotation=45,
    ha="right"
)

plt.tight_layout()
plt.show()


# ============================================================
# 6. STAT 2
# CLUSTERS AVEC UNE LIGNE
# VS
# CLUSTERS AVEC PLUSIEURS LIGNES
# ============================================================

# On calcule combien de lignes existent
# dans chaque cluster pour chaque matching code

cluster_sizes = (
    work
    .groupby(
        [
            MATCHING_CODE_COL,
            CLUSTER_COL
        ],
        dropna=False
    )
    .size()
    .rename("rows_in_cluster")
    .reset_index()
)


# Classification du cluster

cluster_sizes["cluster_type"] = np.where(
    cluster_sizes["rows_in_cluster"] == 1,
    "single_line",
    "multiple_lines"
)


# Nombre de clusters de chaque type
# par matching code

stat_cluster_type = (
    cluster_sizes
    .groupby(
        [
            MATCHING_CODE_COL,
            "cluster_type"
        ]
    )
    .size()
    .rename("number_of_clusters")
    .reset_index()
)


cluster_pivot = (
    stat_cluster_type
    .pivot(
        index=MATCHING_CODE_COL,
        columns="cluster_type",
        values="number_of_clusters"
    )
    .fillna(0)
    .astype(int)
)


# Pour être sûr que les deux colonnes existent

for col in [
    "single_line",
    "multiple_lines"
]:
    if col not in cluster_pivot.columns:
        cluster_pivot[col] = 0


cluster_pivot = cluster_pivot[
    [
        "single_line",
        "multiple_lines"
    ]
]


cluster_pivot["total_clusters"] = (
    cluster_pivot["single_line"]
    +
    cluster_pivot["multiple_lines"]
)


cluster_pivot["duplicate_rate_pct"] = (
    cluster_pivot["multiple_lines"]
    /
    cluster_pivot["total_clusters"]
    * 100
).round(2)


print("\n")
print("=" * 70)
print("STAT 2 - CLUSTERS SINGLE VS MULTIPLE")
print("=" * 70)

display(
    cluster_pivot
    .sort_values(
        "total_clusters",
        ascending=False
    )
)


cluster_pivot[
    [
        "single_line",
        "multiple_lines"
    ]
].plot(
    kind="bar",
    stacked=True,
    figsize=(12, 6)
)

plt.title(
    f"Clusters 1 ligne vs plusieurs lignes - {SOURCE}"
)

plt.xlabel("Matching code")
plt.ylabel("Nombre de clusters")

plt.xticks(
    rotation=45,
    ha="right"
)

plt.legend(
    title="Type de cluster"
)

plt.tight_layout()
plt.show()


# ============================================================
# 7. ZOOM SUR UN MATCHING CODE
# ============================================================

# Exemple : matching code 105

MATCHING_CODE_TO_INSPECT = 105


zoom = cluster_sizes[
    cluster_sizes[MATCHING_CODE_COL]
    .astype(str)
    .eq(
        str(MATCHING_CODE_TO_INSPECT)
    )
].copy()


print("\n")
print("=" * 70)
print(
    f"ZOOM MATCHING CODE "
    f"{MATCHING_CODE_TO_INSPECT}"
)
print("=" * 70)


display(
    zoom
    .sort_values(
        "rows_in_cluster",
        ascending=False
    )
)


single_count = (
    zoom["rows_in_cluster"] == 1
).sum()


duplicate_count = (
    zoom["rows_in_cluster"] > 1
).sum()


print(
    f"Clusters à une seule ligne : "
    f"{single_count}"
)

print(
    f"Clusters avec plusieurs lignes : "
    f"{duplicate_count}"
)


# ============================================================
# 8. NORMALISATION DES PAYS
# ============================================================

def normalize_country(series):

    return (
        series
        .astype("string")
        .str.strip()
        .str.upper()
        .replace(
            {
                "": pd.NA,
                "NAN": pd.NA,
                "NONE": pd.NA,
                "<NA>": pd.NA
            }
        )
    )


country_work = work.copy()


country_work["_tpn_country"] = (
    normalize_country(
        country_work[TPN_COUNTRY_COL]
    )
)


country_work["_source_country"] = (
    normalize_country(
        country_work[SOURCE_COUNTRY_COL]
    )
)


# ============================================================
# 9. SAME / DIFFERENT / MISSING
# ============================================================

country_work["country_status"] = np.select(

    [
        country_work["_tpn_country"].isna()
        |
        country_work["_source_country"].isna(),

        country_work["_tpn_country"].eq(
            country_work["_source_country"]
        )
    ],

    [
        "missing_country",
        "same"
    ],

    default="different"
)


print("\n")
print("=" * 70)
print("APERÇU COMPARAISON COUNTRY")
print("=" * 70)


display(
    country_work[
        [
            TPN_ID_COL,
            MATCHING_CODE_COL,
            CLUSTER_COL,
            TPN_COUNTRY_COL,
            SOURCE_COUNTRY_COL,
            "country_status"
        ]
    ].head(20)
)


# ============================================================
# 10. STAT 3A
# COUNTRY TPN VS COUNTRY SOURCE
# VUE GLOBALE
# ============================================================

country_global = (
    country_work
    .groupby(
        [
            MATCHING_CODE_COL,
            "country_status"
        ]
    )
    .size()
    .rename("number_of_matchings")
    .reset_index()
)


country_global_pivot = (
    country_global
    .pivot(
        index=MATCHING_CODE_COL,
        columns="country_status",
        values="number_of_matchings"
    )
    .fillna(0)
    .astype(int)
)


for col in [
    "same",
    "different",
    "missing_country"
]:
    if col not in country_global_pivot.columns:
        country_global_pivot[col] = 0


country_global_pivot = (
    country_global_pivot[
        [
            "same",
            "different",
            "missing_country"
        ]
    ]
)


print("\n")
print("=" * 70)
print("STAT 3A - COMPARAISON COUNTRY - GLOBAL")
print("=" * 70)

display(country_global_pivot)


country_global_pivot.plot(
    kind="bar",
    stacked=True,
    figsize=(12, 6)
)

plt.title(
    f"Country TPN vs Country Source - {SOURCE} - Global"
)

plt.xlabel("Matching code")
plt.ylabel("Nombre de matchings")

plt.xticks(
    rotation=45,
    ha="right"
)

plt.legend(
    title="Country comparison"
)

plt.tight_layout()
plt.show()


# ============================================================
# 11. IDENTIFICATION DES CLUSTERS DUPLIQUÉS
# ============================================================

duplicate_cluster_keys = (
    cluster_sizes.loc[
        cluster_sizes["rows_in_cluster"] > 1,
        [
            MATCHING_CODE_COL,
            CLUSTER_COL
        ]
    ]
    .copy()
)


# On récupère uniquement les lignes
# appartenant à ces clusters

country_duplicates = (
    country_work
    .merge(
        duplicate_cluster_keys,
        on=[
            MATCHING_CODE_COL,
            CLUSTER_COL
        ],
        how="inner"
    )
)


print(
    "\nNombre de lignes appartenant "
    "à des clusters dupliqués :",
    len(country_duplicates)
)


# ============================================================
# 12. STAT 3B
# COUNTRY TPN VS COUNTRY SOURCE
# UNIQUEMENT POUR LES DOUBLONS
# ============================================================

country_duplicates_stat = (
    country_duplicates
    .groupby(
        [
            MATCHING_CODE_COL,
            "country_status"
        ]
    )
    .size()
    .rename("number_of_matchings")
    .reset_index()
)


country_duplicates_pivot = (
    country_duplicates_stat
    .pivot(
        index=MATCHING_CODE_COL,
        columns="country_status",
        values="number_of_matchings"
    )
    .fillna(0)
    .astype(int)
)


for col in [
    "same",
    "different",
    "missing_country"
]:
    if col not in country_duplicates_pivot.columns:
        country_duplicates_pivot[col] = 0


country_duplicates_pivot = (
    country_duplicates_pivot[
        [
            "same",
            "different",
            "missing_country"
        ]
    ]
)


print("\n")
print("=" * 70)
print(
    "STAT 3B - COMPARAISON COUNTRY "
    "- CLUSTERS DUPLIQUÉS"
)
print("=" * 70)

display(country_duplicates_pivot)


country_duplicates_pivot.plot(
    kind="bar",
    stacked=True,
    figsize=(12, 6)
)

plt.title(
    f"Country TPN vs Country Source - "
    f"{SOURCE} - Clusters dupliqués"
)

plt.xlabel("Matching code")
plt.ylabel("Nombre de matchings")

plt.xticks(
    rotation=45,
    ha="right"
)

plt.legend(
    title="Country comparison"
)

plt.tight_layout()
plt.show()


# ============================================================
# 13. TABLEAU RÉCAPITULATIF
# ============================================================

summary = (
    stat_matching_code
    .set_index(MATCHING_CODE_COL)

    .join(
        cluster_pivot,
        how="outer"
    )

    .join(
        country_global_pivot
        .add_prefix("country_"),
        how="outer"
    )

    .fillna(0)
)


print("\n")
print("=" * 70)
print("TABLEAU RÉCAPITULATIF FINAL")
print("=" * 70)


display(
    summary
    .sort_values(
        "number_of_matchings",
        ascending=False
    )
)


# ============================================================
# 14. EXPORT OPTIONNEL
# ============================================================

# Décommenter si besoin

# stat_matching_code.to_csv(
#     "01_matching_by_code.csv",
#     index=False
# )

# cluster_pivot.to_csv(
#     "02_clusters_single_vs_multiple.csv"
# )

# country_global_pivot.to_csv(
#     "03_country_global.csv"
# )

# country_duplicates_pivot.to_csv(
#     "04_country_duplicates.csv"
# )

# summary.to_csv(
#     "05_summary.csv"
# )
