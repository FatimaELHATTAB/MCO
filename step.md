# SPLINK 
# ==============================================================

# Partie 1 : Comprendre comment fonctionne Splink
# -----------------------------------------------
"""
Splink est une librairie open-source développée par le Ministry of Justice du Royaume-Uni. Elle permet de faire de la détection de doublons et de la correspondance entre entités dans de grands jeux de données, même en présence de variations ou d'erreurs (fuzzy matching).

### Fonctionnement global (avec exemples) :
- Splink crée toutes les paires possibles de lignes entre deux datasets (ou dans un même dataset pour les doublons)
    ➤ Exemple : 10 000 lignes dans chaque dataset → 100 millions de paires (10k x 10k)

- Il applique des règles de "blocking" pour éviter la combinatoire explosive
    ➤ Exemple : si on bloque sur le pays (FR uniquement), on réduit à ~1 million de paires pertinentes

- Il compare les colonnes en utilisant différentes méthodes (exact match, Jaro-Winkler, Levenshtein, etc.)
    ➤ Exemple : comparer "BNP Paribas" à "BNP Paribas SA" avec JaroWinkler → score 0.96

- Il utilise un modèle probabiliste de type Fellegi-Sunter pour calculer une **probabilité de correspondance** pour chaque paire
    ➤ Exemple : une paire peut avoir une probabilité de match = 0.94 si les noms et pays correspondent mais pas l'ISIN

- Il regroupe les paires matchées en **clusters d'entités uniques** (chaînage transitif ou composantes connexes)
    ➤ Exemple : si A=B et B=C, alors A, B et C seront dans le même cluster

### Probabilités utilisées :
- **m-probabilité** : probabilité qu'un champ soit égal si les deux lignes sont un vrai match
- **u-probabilité** : probabilité qu'un champ soit égal par hasard si les lignes ne matchent pas
- Le **score log(m/u)** donne un poids d'information pour chaque comparaison

---

# Partie 1 bis : Algorithme EM (Expectation-Maximization)

L’algorithme EM est utilisé par Splink pour **estimer automatiquement les probabilités m et u** à partir des données.

- **Étape 1 – Initialisation** : Splink fait une hypothèse initiale naïve sur les matches probables
- **Étape 2 – E-step (Expectation)** : à partir des poids, il estime les probabilités de match de chaque paire
- **Étape 3 – M-step (Maximization)** : à partir des matches estimés, il réajuste les m/u pour chaque niveau de comparaison
- **Répète** jusqu’à convergence

➤ Exemple : au bout de 5 itérations, Splink comprend que "Total" et "TotalEnergies" sont souvent liés, et ajuste le poids de cette ressemblance

---

# Partie 1 ter : Performance et Scalabilité

Splink est conçu pour des cas d’usage à grande échelle. Ses performances dépendent de trois leviers principaux :

### 1. **Blocking Rules**
Réduisent le nombre de comparaisons nécessaires. Sans blocking, comparer 1M × 1M = 1 trillion de paires. Avec un bon blocking (ex : même pays), on peut ramener ça à 5M ou 10M.

### 2. **Backend moteur SQL (DuckDB, Spark)**
- **DuckDB** (par défaut) : parfait pour prototypage local et datasets < 10 millions de lignes
- **Spark** : permet d'exécuter Splink sur des clusters distribués pour gérer des dizaines ou centaines de millions de lignes

### 3. **Structure en paires + vectorisation**
Splink ne compare pas des lignes naïvement. Il transforme les paires en vecteurs de comparaison (ex : 1 si égal, 0.8 si proche, etc.) et les combine de façon mathématiquement optimisée

### Exemple concret :
| Volume initial | Blocking appliqué     | Comparaisons finales |
|----------------|------------------------|----------------------|
| 10 000 × 10 000| sur `country`, `LEI`   | ≈ 1 200 000          |
| 1 000 000 × 1 000 000| aucun blocking | ≈ 1 000 000 000 000 (infaisable)

Conclusion : **bien définir ses rules de blocking** est la clé pour tirer le meilleur de Splink à l’échelle

---

### Comparaison des deux éléments clés :

| Élément             | blocking_rules                                 | comparison_columns                              |
|---------------------|--------------------------------------------------|---------------------------------------------------|
| Rôle                | Réduit le nombre de paires à comparer            | Calcule un score ou une distance                  |
| Syntaxe             | SQL-like condition entre l. et r.                | Fonctions Splink (ExactMatch, JaroWinkler, ...)  |
| Appliqué quand ?    | En premier, avant toute comparaison              | Ensuite, sur les paires retenues                  |
| But principal       | Optimiser les perfs, éviter les combinaisons inutiles | Décider si deux lignes représentent le même objet |

### Méthodes de comparaison possibles :
- **ExactMatch** : égalité stricte (string, code, etc.)
- **JaroWinkler** : distance floue pour noms proches (Michel/Michele)
- **LevenshteinAtThresholds** : tolérance aux typos avec seuils
- **ArrayIntersect** : pour listes de valeurs (tags, codes multiples)
- **TermFrequencyAdjustedExactMatch** : prend en compte la fréquence du terme (TF-IDF)



# 🧠 Comparatif des Méthodes d'Entity Resolution

## 1. 🗂️ Tableau comparatif global

| Méthode          | Approche                 | Supervision       | Scalabilité      | Fuzzy Matching | Limites clés |
|------------------|--------------------------|-------------------|------------------|----------------|--------------|
| **Splink**       | Probabiliste (Fellegi-Sunter) + règles | ❌ Non supervisé | ✅✅✅ Spark/DuckDB | ✅✅ (via distances textuelles) | Setup initial complexe, JSON à configurer |
| **dedupe**       | Apprentissage actif (régression logistique) | ✅ Semi-supervisé | ✅ (limité au RAM, <20M) | ✅✅✅ (TF-IDF, Levenshtein, etc.) | Nécessite annotation manuelle initiale |
| **RecordLinkage**| Règles + probabiliste    | ❌ Non supervisé  | ❌ (<1M lignes recommandé) | ✅ (simple) | RAM-bound, lent pour gros volumes |
| **Zingg**        | Active learning + Spark  | ✅ Semi-supervisé | ✅✅ Spark-native  | ✅✅            | Moins connu, moins documenté |
| **DeepMatcher**  | Deep Learning supervisé  | ✅✅ Supervision forte | ❌ (besoin GPU, gros dataset) | ✅✅✅ | Données annotées massives requises |
| **FastLink**     | Probabiliste (Fellegi)   | ❌ Non supervisé  | ✅ (R-based)      | ✅              | Moins adapté à Python / pipelines ML |
| **PyJedAI**      | Ensemble hybride         | ✅                | ✅                | ✅              | Plus académique, peu intégré aux pipelines |

---

## 2. ✅  Splink/dedupe 

| Critère métier / technique                   | Raisons de pertinence |
|---------------------------------------------|------------------------|
| **Volumétrie élevée (100M+ lignes)**         | Seuls Splink (Spark/DuckDB) ou Zingg le gèrent bien |
| **Règles métiers explicites (ex: ISIN, LEI)**| Splink permet de coder des `comparison_levels` très précises |
| **1 seul champ à fuzzy matcher**            | Splink et dedupe gèrent très bien cela (Jaro-Winkler, TF-IDF…) |
| **Besoin de scoring et cluster_id en sortie**| Les deux outils produisent des scores et des clusters interprétables |
| **Pipeline reproductible et industrialisable**| Splink a une structure plus robuste pour l'industrialisation |
| **Pas besoin d’un modèle black-box**        | Splink offre une explicabilité métier claire |
| **Possibilité de supervision légère**        | dedupe permet d’apprendre à partir de peu d’annotations |

---

## 3. 🤜 Splink vs 🤛 dedupe 

| Critère                            | **Splink**                                      | **dedupe**                                   |
|------------------------------------|-------------------------------------------------|-----------------------------------------------|
| **Approche**                       | Modèle probabiliste statique (Fellegi-Sunter)  | Apprentissage semi-supervisé (régression logistique) |
| **Fuzzy matching**                 | Comparisons manuelles (Jaro-Winkler, etc.)     | Automatique via apprentissage                 |
| **Règles exactes (ISIN, LEI, etc.)**| Définies explicitement dans les settings       | Apprises si tu as annoté des exemples         |
| **Scalabilité**                    | ✅ Spark / DuckDB pour des millions de lignes  | ❌ RAM-bound (~1-10M lignes max efficacement)  |
| **Supervision**                    | ❌ Aucune (logique déclarative uniquement)      | ✅ Annotation active (match / non-match)       |
| **Blocking (réduction de la combinatoire)** | ✅ Très flexible, obligatoire et optimisé     | ✅ Automatique ou manuel, mais moins flexible  |
| **Explicabilité**                  | ✅ Très élevée (chaque règle est lisible)       | Moyenne (poids appris, moins lisibles)        |
| **Mise en production**             | ✅ Exécution SQL, pipeline industrialisable     | ✅ Possible en script Python, modèle exportable |
| **Setup initial**                  | ❌ Plus complexe (`settings.json`)              | ✅ Simple à configurer                         |
| **Prise en main rapide**           | ❌ Apprentissage nécessaire                     | ✅ Rapide, très bon pour POC                   |

---

## 🏆  quel outil choisir ?

| Situation typique | Outil recommandé |
|-------------------|------------------|
| **Gros volume de données (100M+), règles précises, un champ flou, pipeline maîtrisé et traçable** | ✅✅ **Splink** |
| **Dataset < 10M lignes, champ flou difficile, besoin d’un apprentissage intelligent, POC rapide** | ✅ **dedupe** |
| **Besoin d’un pipeline long terme, compatible Spark ou SQL, exportable et explicable** | ✅✅ **Splink** |

---





