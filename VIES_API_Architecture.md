# Remédiation des TPNID avec plusieurs lignes ouvertes

## Objectif

Pour un même `tpn_id`, il ne doit y avoir qu'une seule ligne ouverte.

Une ligne est considérée comme **ouverte** lorsque :

```sql
closed_at IS NULL
```

La remédiation repose sur 4 cas métier.

> **Important**
>
> Dans la capture, `MATCHING_ORBIS` semble apparaître dans `tax_id_status`, alors que `created_by`
> contient plutôt `VAT_ENRICHMENT`, `TVA_BATCH`, `MANUAL_REMEDIATION`.
>
> Les requêtes ci-dessous suivent cependant la logique décrite avec :
>
> ```sql
> created_by = 'MATCHING_ORBIS'
> ```
>
> Si `MATCHING_ORBIS` doit en réalité être recherché dans `tax_id_status`, il faut remplacer
> les conditions correspondantes.

---

# 1. Détection globale des TPNID ayant plusieurs lignes ouvertes

```sql
SELECT
    tpn_id,
    COUNT(*) AS nb_open_lines
FROM tax_table
WHERE closed_at IS NULL
GROUP BY tpn_id
HAVING COUNT(*) > 1;
```

---

# 2. CAS 1 — Toutes les lignes ouvertes sont MATCHING_ORBIS

## Règle métier

Exemple :

```text
TPN1   MATCHING_ORBIS   NULL
TPN1   MATCHING_ORBIS   NULL
TPN1   MATCHING_ORBIS   NULL
```

Toutes les lignes ouvertes sont créées par `MATCHING_ORBIS`.

Action :

```text
DELETE de toutes ces lignes ouvertes
```

## Détection

```sql
SELECT
    tpn_id,
    COUNT(*) AS nb_open_lines
FROM tax_table
WHERE closed_at IS NULL
GROUP BY tpn_id
HAVING COUNT(*) > 1
   AND SUM(
       CASE
           WHEN created_by = 'MATCHING_ORBIS' THEN 1
           ELSE 0
       END
   ) = COUNT(*);
```

## Voir les lignes concernées avant suppression

```sql
SELECT t.*
FROM tax_table t
WHERE t.closed_at IS NULL
  AND t.tpn_id IN (
      SELECT tpn_id
      FROM tax_table
      WHERE closed_at IS NULL
      GROUP BY tpn_id
      HAVING COUNT(*) > 1
         AND SUM(
             CASE
                 WHEN created_by = 'MATCHING_ORBIS' THEN 1
                 ELSE 0
             END
         ) = COUNT(*)
  );
```

## Remédiation

```sql
DELETE FROM tax_table t
WHERE t.closed_at IS NULL
  AND t.tpn_id IN (
      SELECT tpn_id
      FROM tax_table
      WHERE closed_at IS NULL
      GROUP BY tpn_id
      HAVING COUNT(*) > 1
         AND SUM(
             CASE
                 WHEN created_by = 'MATCHING_ORBIS' THEN 1
                 ELSE 0
             END
         ) = COUNT(*)
  );
```

> Attention : avec cette règle, après suppression, le `tpn_id` concerné n'aura plus aucune ligne ouverte.

---

# 3. CAS 2 — Une seule ligne MANUAL_REMEDIATION

## Règle métier

Exemple :

```text
TPN2   MANUAL_REMEDIATION   NULL   <-- reste ouverte
TPN2   MATCHING_ORBIS       NULL   <-- fermeture
TPN2   TVA_BATCH            NULL   <-- fermeture
```

Action :

- conserver la ligne `MANUAL_REMEDIATION` ouverte ;
- fermer toutes les autres lignes ouvertes ;
- renseigner :
  - `closed_at = CURRENT_TIMESTAMP`
  - `closed_by = 'VAT_ENRICHMENT'`

## Détection

```sql
SELECT
    tpn_id,
    COUNT(*) AS nb_open_lines
FROM tax_table
WHERE closed_at IS NULL
GROUP BY tpn_id
HAVING COUNT(*) > 1
   AND SUM(
       CASE
           WHEN created_by = 'MANUAL_REMEDIATION' THEN 1
           ELSE 0
       END
   ) = 1;
```

## Remédiation

```sql
UPDATE tax_table t
SET
    closed_at = CURRENT_TIMESTAMP,
    closed_by = 'VAT_ENRICHMENT'
WHERE t.closed_at IS NULL
  AND (
      t.created_by <> 'MANUAL_REMEDIATION'
      OR t.created_by IS NULL
  )
  AND t.tpn_id IN (
      SELECT tpn_id
      FROM tax_table
      WHERE closed_at IS NULL
      GROUP BY tpn_id
      HAVING COUNT(*) > 1
         AND SUM(
             CASE
                 WHEN created_by = 'MANUAL_REMEDIATION' THEN 1
                 ELSE 0
             END
         ) = 1
  );
```

---

# 4. CAS 3 — Une seule ligne MATCHING_ORBIS + ligne(s) TVA_BATCH

## Règle métier

Exemple :

```text
TPN3   MATCHING_ORBIS   NULL   <-- reste ouverte
TPN3   TVA_BATCH        NULL   <-- fermeture
```

Action :

- conserver la ligne `MATCHING_ORBIS` ouverte ;
- fermer les lignes `TVA_BATCH`.

## Détection

```sql
SELECT
    tpn_id,
    COUNT(*) AS nb_open_lines
FROM tax_table
WHERE closed_at IS NULL
GROUP BY tpn_id
HAVING COUNT(*) > 1

   AND SUM(
       CASE
           WHEN created_by = 'MATCHING_ORBIS' THEN 1
           ELSE 0
       END
   ) = 1

   AND SUM(
       CASE
           WHEN created_by = 'TVA_BATCH' THEN 1
           ELSE 0
       END
   ) >= 1

   AND SUM(
       CASE
           WHEN created_by = 'MANUAL_REMEDIATION' THEN 1
           ELSE 0
       END
   ) = 0;
```

## Remédiation

```sql
UPDATE tax_table t
SET
    closed_at = CURRENT_TIMESTAMP,
    closed_by = 'VAT_ENRICHMENT'
WHERE t.closed_at IS NULL
  AND t.created_by = 'TVA_BATCH'
  AND t.tpn_id IN (
      SELECT tpn_id
      FROM tax_table
      WHERE closed_at IS NULL
      GROUP BY tpn_id
      HAVING COUNT(*) > 1

         AND SUM(
             CASE
                 WHEN created_by = 'MATCHING_ORBIS' THEN 1
                 ELSE 0
             END
         ) = 1

         AND SUM(
             CASE
                 WHEN created_by = 'TVA_BATCH' THEN 1
                 ELSE 0
             END
         ) >= 1

         AND SUM(
             CASE
                 WHEN created_by = 'MANUAL_REMEDIATION' THEN 1
                 ELSE 0
             END
         ) = 0
  );
```

---

# 5. CAS 4 — Plusieurs lignes MATCHING_ORBIS + une seule ligne TVA_BATCH

## Règle métier

Exemple :

```text
TPN4   MATCHING_ORBIS   NULL   <-- fermeture
TPN4   MATCHING_ORBIS   NULL   <-- fermeture
TPN4   TVA_BATCH        NULL   <-- reste ouverte
```

Action :

- conserver la ligne `TVA_BATCH` ouverte ;
- fermer les lignes `MATCHING_ORBIS`.

## Détection

```sql
SELECT
    tpn_id,
    COUNT(*) AS nb_open_lines
FROM tax_table
WHERE closed_at IS NULL
GROUP BY tpn_id
HAVING COUNT(*) > 1

   AND SUM(
       CASE
           WHEN created_by = 'MATCHING_ORBIS' THEN 1
           ELSE 0
       END
   ) > 1

   AND SUM(
       CASE
           WHEN created_by = 'TVA_BATCH' THEN 1
           ELSE 0
       END
   ) = 1

   AND SUM(
       CASE
           WHEN created_by = 'MANUAL_REMEDIATION' THEN 1
           ELSE 0
       END
   ) = 0;
```

## Remédiation

```sql
UPDATE tax_table t
SET
    closed_at = CURRENT_TIMESTAMP,
    closed_by = 'VAT_ENRICHMENT'
WHERE t.closed_at IS NULL
  AND t.created_by = 'MATCHING_ORBIS'
  AND t.tpn_id IN (
      SELECT tpn_id
      FROM tax_table
      WHERE closed_at IS NULL
      GROUP BY tpn_id
      HAVING COUNT(*) > 1

         AND SUM(
             CASE
                 WHEN created_by = 'MATCHING_ORBIS' THEN 1
                 ELSE 0
             END
         ) > 1

         AND SUM(
             CASE
                 WHEN created_by = 'TVA_BATCH' THEN 1
                 ELSE 0
             END
         ) = 1

         AND SUM(
             CASE
                 WHEN created_by = 'MANUAL_REMEDIATION' THEN 1
                 ELSE 0
             END
         ) = 0
  );
```

---

# 6. Requête unique de classification des cas

Cette requête permet de voir tous les `tpn_id` ayant plusieurs lignes ouvertes et de les classer.

```sql
SELECT
    tpn_id,
    COUNT(*) AS nb_open,

    SUM(
        CASE
            WHEN created_by = 'MATCHING_ORBIS' THEN 1
            ELSE 0
        END
    ) AS nb_matching_orbis,

    SUM(
        CASE
            WHEN created_by = 'TVA_BATCH' THEN 1
            ELSE 0
        END
    ) AS nb_tva_batch,

    SUM(
        CASE
            WHEN created_by = 'MANUAL_REMEDIATION' THEN 1
            ELSE 0
        END
    ) AS nb_manual_remediation,

    CASE

        WHEN
            SUM(
                CASE
                    WHEN created_by = 'MATCHING_ORBIS' THEN 1
                    ELSE 0
                END
            ) = COUNT(*)
        THEN 'CASE_1_ALL_ORBIS'

        WHEN
            SUM(
                CASE
                    WHEN created_by = 'MANUAL_REMEDIATION' THEN 1
                    ELSE 0
                END
            ) = 1
        THEN 'CASE_2_MANUAL'

        WHEN
            SUM(
                CASE
                    WHEN created_by = 'MATCHING_ORBIS' THEN 1
                    ELSE 0
                END
            ) = 1
            AND
            SUM(
                CASE
                    WHEN created_by = 'TVA_BATCH' THEN 1
                    ELSE 0
                END
            ) >= 1
        THEN 'CASE_3_ONE_ORBIS'

        WHEN
            SUM(
                CASE
                    WHEN created_by = 'MATCHING_ORBIS' THEN 1
                    ELSE 0
                END
            ) > 1
            AND
            SUM(
                CASE
                    WHEN created_by = 'TVA_BATCH' THEN 1
                    ELSE 0
                END
            ) = 1
        THEN 'CASE_4_MULTIPLE_ORBIS'

        ELSE 'CASE_NOT_COVERED'

    END AS remediation_case

FROM tax_table
WHERE closed_at IS NULL
GROUP BY tpn_id
HAVING COUNT(*) > 1;
```

## Pourquoi conserver CASE_NOT_COVERED ?

Les cas non couverts ne doivent pas être corrigés automatiquement.

Ils doivent être inspectés manuellement avant toute modification.

---

# 7. Contrôle SQL après remédiation

Après les corrections :

```sql
SELECT
    tpn_id,
    COUNT(*) AS nb_open_lines
FROM tax_table
WHERE closed_at IS NULL
GROUP BY tpn_id
HAVING COUNT(*) > 1;
```

Si la remédiation a couvert tous les cas attendus, cette requête ne doit plus retourner de lignes,
sauf éventuellement les cas volontairement laissés en `CASE_NOT_COVERED`.

---

# 8. Exécution sécurisée dans une transaction PostgreSQL

Avant de lancer les `UPDATE` ou `DELETE` en production :

```sql
BEGIN;
```

Exécuter ensuite les requêtes de remédiation.

Contrôler le résultat avec :

```sql
SELECT
    tpn_id,
    COUNT(*) AS nb_open_lines
FROM tax_table
WHERE closed_at IS NULL
GROUP BY tpn_id
HAVING COUNT(*) > 1;
```

Si le résultat n'est pas conforme :

```sql
ROLLBACK;
```

Si le résultat est conforme :

```sql
COMMIT;
```

---

# 9. Script Python / Pandas

```python
import pandas as pd
from datetime import datetime, timezone


# ====================================================
# PARAMETRAGE
# ====================================================

TPN_COLUMN = "tpn_id"
CREATED_BY_COLUMN = "created_by"
CLOSED_AT_COLUMN = "closed_at"
CLOSED_BY_COLUMN = "closed_by"

MATCHING_ORBIS = "MATCHING_ORBIS"
TVA_BATCH = "TVA_BATCH"
MANUAL_REMEDIATION = "MANUAL_REMEDIATION"

CLOSED_BY_VALUE = "VAT_ENRICHMENT"


# ====================================================
# 1. PREPARATION
# ====================================================

# Exemple :
# df = pd.read_csv("input.csv")

# Si closed_at contient "-", "", "NULL", etc.
df[CLOSED_AT_COLUMN] = df[CLOSED_AT_COLUMN].replace(
    ["-", "", "NULL", "null"],
    pd.NA
)

# Lignes ouvertes
open_mask = df[CLOSED_AT_COLUMN].isna()

open_df = df.loc[open_mask].copy()


# ====================================================
# 2. TPNID AYANT PLUSIEURS LIGNES OUVERTES
# ====================================================

nb_open = (
    open_df
    .groupby(TPN_COLUMN)
    .size()
)

duplicate_tpnids = nb_open[
    nb_open > 1
].index

duplicate_open = open_df[
    open_df[TPN_COLUMN].isin(duplicate_tpnids)
].copy()


# ====================================================
# 3. STATISTIQUES PAR TPNID
# ====================================================

stats = (
    duplicate_open
    .groupby(TPN_COLUMN)
    .agg(
        nb_open=(TPN_COLUMN, "size"),

        nb_matching_orbis=(
            CREATED_BY_COLUMN,
            lambda x: (x == MATCHING_ORBIS).sum()
        ),

        nb_tva_batch=(
            CREATED_BY_COLUMN,
            lambda x: (x == TVA_BATCH).sum()
        ),

        nb_manual_remediation=(
            CREATED_BY_COLUMN,
            lambda x: (x == MANUAL_REMEDIATION).sum()
        )
    )
    .reset_index()
)


# ====================================================
# 4. CLASSIFICATION
# ====================================================

def classify(row):

    # CAS 1 :
    # toutes les lignes ouvertes sont MATCHING_ORBIS
    if row["nb_matching_orbis"] == row["nb_open"]:
        return "CASE_1_ALL_ORBIS"

    # CAS 2 :
    # exactement une ligne MANUAL_REMEDIATION
    if row["nb_manual_remediation"] == 1:
        return "CASE_2_MANUAL"

    # CAS 3 :
    # exactement une ligne MATCHING_ORBIS
    # et au moins une TVA_BATCH
    if (
        row["nb_matching_orbis"] == 1
        and row["nb_tva_batch"] >= 1
    ):
        return "CASE_3_ONE_ORBIS"

    # CAS 4 :
    # plusieurs MATCHING_ORBIS
    # et exactement une TVA_BATCH
    if (
        row["nb_matching_orbis"] > 1
        and row["nb_tva_batch"] == 1
    ):
        return "CASE_4_MULTIPLE_ORBIS"

    return "CASE_NOT_COVERED"


stats["case"] = stats.apply(
    classify,
    axis=1
)


# ====================================================
# 5. AFFICHAGE DES CAS DETECTES
# ====================================================

print("\n===== CLASSIFICATION =====")
print(stats)

print("\n===== CASE_NOT_COVERED =====")
print(
    stats[
        stats["case"] == "CASE_NOT_COVERED"
    ]
)


# ====================================================
# 6. DATE DE FERMETURE
# ====================================================

now = datetime.now(timezone.utc)


# ====================================================
# 7. CAS 1
# DELETE DES LIGNES MATCHING_ORBIS
# ====================================================

case1_tpnids = stats.loc[
    stats["case"] == "CASE_1_ALL_ORBIS",
    TPN_COLUMN
]

mask_case1 = (
    df[TPN_COLUMN].isin(case1_tpnids)
    & df[CLOSED_AT_COLUMN].isna()
)

df = df.loc[
    ~mask_case1
].copy()


# ====================================================
# 8. CAS 2
# GARDER MANUAL_REMEDIATION
# FERMER TOUTES LES AUTRES
# ====================================================

case2_tpnids = stats.loc[
    stats["case"] == "CASE_2_MANUAL",
    TPN_COLUMN
]

mask_case2 = (
    df[TPN_COLUMN].isin(case2_tpnids)
    & df[CLOSED_AT_COLUMN].isna()
    & (
        df[CREATED_BY_COLUMN] != MANUAL_REMEDIATION
    )
)

df.loc[
    mask_case2,
    CLOSED_AT_COLUMN
] = now

df.loc[
    mask_case2,
    CLOSED_BY_COLUMN
] = CLOSED_BY_VALUE


# ====================================================
# 9. CAS 3
# 1 MATCHING_ORBIS
# FERMER TVA_BATCH
# ====================================================

case3_tpnids = stats.loc[
    stats["case"] == "CASE_3_ONE_ORBIS",
    TPN_COLUMN
]

mask_case3 = (
    df[TPN_COLUMN].isin(case3_tpnids)
    & df[CLOSED_AT_COLUMN].isna()
    & (
        df[CREATED_BY_COLUMN] == TVA_BATCH
    )
)

df.loc[
    mask_case3,
    CLOSED_AT_COLUMN
] = now

df.loc[
    mask_case3,
    CLOSED_BY_COLUMN
] = CLOSED_BY_VALUE


# ====================================================
# 10. CAS 4
# PLUSIEURS MATCHING_ORBIS + 1 TVA_BATCH
# FERMER LES MATCHING_ORBIS
# ====================================================

case4_tpnids = stats.loc[
    stats["case"] == "CASE_4_MULTIPLE_ORBIS",
    TPN_COLUMN
]

mask_case4 = (
    df[TPN_COLUMN].isin(case4_tpnids)
    & df[CLOSED_AT_COLUMN].isna()
    & (
        df[CREATED_BY_COLUMN] == MATCHING_ORBIS
    )
)

df.loc[
    mask_case4,
    CLOSED_AT_COLUMN
] = now

df.loc[
    mask_case4,
    CLOSED_BY_COLUMN
] = CLOSED_BY_VALUE


# ====================================================
# 11. CONTROLE FINAL
# ====================================================

remaining_open = (
    df[
        df[CLOSED_AT_COLUMN].isna()
    ]
    .groupby(TPN_COLUMN)
    .size()
)

remaining_errors = remaining_open[
    remaining_open > 1
]

print(
    "\n===== TPNID AYANT ENCORE PLUSIEURS LIGNES OUVERTES ====="
)

print(remaining_errors)


if remaining_errors.empty:
    print(
        "\nOK : aucun TPNID ne possède plusieurs lignes ouvertes."
    )
else:
    print(
        "\nATTENTION : certains TPNID doivent encore être contrôlés."
    )


# ====================================================
# 12. EXPORT OPTIONNEL
# ====================================================

# Exemple :
# df.to_csv(
#     "tax_table_remediated.csv",
#     index=False
# )
```

---

# 10. Version Python : détection uniquement, sans modifier les données

Si l'on souhaite d'abord analyser les cas sans appliquer de correction :

```python
import pandas as pd

df["closed_at"] = df["closed_at"].replace(
    ["-", "", "NULL", "null"],
    pd.NA
)

open_df = df[
    df["closed_at"].isna()
].copy()

duplicate_open = open_df[
    open_df.groupby("tpn_id")["tpn_id"].transform("size") > 1
].copy()

stats = (
    duplicate_open
    .groupby("tpn_id")
    .agg(
        nb_open=("tpn_id", "size"),
        nb_matching_orbis=(
            "created_by",
            lambda x: (x == "MATCHING_ORBIS").sum()
        ),
        nb_tva_batch=(
            "created_by",
            lambda x: (x == "TVA_BATCH").sum()
        ),
        nb_manual_remediation=(
            "created_by",
            lambda x: (x == "MANUAL_REMEDIATION").sum()
        )
    )
    .reset_index()
)

def classify(row):

    if row["nb_matching_orbis"] == row["nb_open"]:
        return "CASE_1_ALL_ORBIS"

    if row["nb_manual_remediation"] == 1:
        return "CASE_2_MANUAL"

    if (
        row["nb_matching_orbis"] == 1
        and row["nb_tva_batch"] >= 1
    ):
        return "CASE_3_ONE_ORBIS"

    if (
        row["nb_matching_orbis"] > 1
        and row["nb_tva_batch"] == 1
    ):
        return "CASE_4_MULTIPLE_ORBIS"

    return "CASE_NOT_COVERED"


stats["case"] = stats.apply(
    classify,
    axis=1
)

print(stats)
```

---

# 11. Résumé de la logique métier

| Cas | Situation | Ligne conservée ouverte | Action |
|---|---|---|---|
| 1 | Toutes les lignes sont `MATCHING_ORBIS` | Aucune | DELETE des lignes ouvertes |
| 2 | Une seule `MANUAL_REMEDIATION` | `MANUAL_REMEDIATION` | Fermer toutes les autres |
| 3 | Une seule `MATCHING_ORBIS` + `TVA_BATCH` | `MATCHING_ORBIS` | Fermer `TVA_BATCH` |
| 4 | Plusieurs `MATCHING_ORBIS` + une seule `TVA_BATCH` | `TVA_BATCH` | Fermer les `MATCHING_ORBIS` |
| Autre | Combinaison non prévue | Aucune correction automatique | Contrôle manuel |

Pour toutes les fermetures :

```text
closed_at = date/heure de la remédiation
closed_by = VAT_ENRICHMENT
```

