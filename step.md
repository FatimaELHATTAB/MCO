# 🧭 Roadmap stratégique – Moteur de Matching d’Entités Légales

> Cette roadmap définit les évolutions du moteur de matching d’entités légales dans une perspective industrielle, modulaire et orchestrable. Elle repose sur une architecture claire, une logique déclarative pilotée par YAML, une exposition API, et une supervision fine via Airflow.

---

## 🎯 Objectifs stratégiques

- Concevoir un moteur de matching pilotable par configuration
- Garantir une supervision fine (provider/scope) et un redémarrage ciblé
- Assurer une testabilité robuste et une séparation des responsabilités
- Favoriser l’intégration dans différents contextes (Airflow, API, Java, CLI)
- Préparer une ouverture vers des approches plus intelligentes (RAG, embeddings, LLM)

---

## ✅ Priorisation globale

| Phase | Élément | Priorité |
|-------|---------|----------|
| 1 | Refonte de l’architecture (arborescence + séparation logique) | 🔴 Critique |
| 2 | Matching piloté par YAML (matching_rules.yaml) | 🔴 Critique |
| 3 | Fuzzy matching interne configurable | 🔴 Critique |
| 4 | API FastAPI exposant le moteur | 🔴 Critique |
| 5 | Orchestration via Airflow DAGs par provider | 🟡 Moyenne |
| 6 | Supervision multi-projet (appel externe Java) | 🟡 Moyenne |
| 7 | Script de génération automatique de provider | 🟡 Moyenne |

---

## 🧱 Phase 1 – Refonte de l’architecture (PRIORITÉ CRITIQUE)

### 🎯 Objectif
Structurer le moteur en couches bien délimitées, orientées testabilité et maintenabilité.

### 📂 Arborescence cible

legal_entity_matching/
├── src/
│ ├── core/
│ │ ├── matcher.py
│ │ ├── fuzzy_matcher.py
│ │ └── postprocessor.py
│ ├── runner/
│ │ └── provider_runner.py
│ └── utils/
├── config/
│ └── matching_rules.yaml
├── api/
│ └── api.py
├── dags/
│ └── matching_xxx_dag.py
├── scripts/
│ ├── run.py
│ └── add_provider.sh
├── tests/
│ ├── test_matcher.py
│ ├── test_fuzzy.py
│ └── test_provider_runner.py



### 🧠 Impacts positifs

- Lisibilité et répartition claire des responsabilités
- Testabilité granulaire par module
- Intégration simplifiée dans Airflow, API, CLI
- Encapsulation parfaite pour appels externes

---

## ⚙️ Phase 2 – Pilotage YAML (matching_rules.yaml)

### Objectif : décrire les règles de matching par provider

- `scope` (pays, conditions SQL-like)
- `levels` (`type: exact` ou `fuzzy`, colonnes, seuils, méthodes)
- `inherit: DEFAULT` pour la mutualisation

### Bénéfices

- Plus besoin de modifier le code pour adapter une logique
- Vision centralisée de l’ensemble des providers
- Contrôle de version du moteur via Git uniquement

---

## 🔁 Phase 3 – Fuzzy matching interne

### Objectif : implémenter un module interne performant

- `fuzzy_matcher.py` avec plusieurs méthodes : Jaro-Winkler, Levenshtein, token_sort
- Seuil piloté par YAML
- Activé seulement si `type: fuzzy`

### Bénéfices

- Suppression des appels externes flous
- Meilleure supervision
- Intégration native au moteur de matching

---

## 🌐 Phase 4 – API FastAPI

### Objectif : exposer le moteur en tant que service REST

- `/match`, `/health`, `/available_providers`
- Wrapper autour de `ProviderRunner`
- Située dans un dossier `api/` propre

### Bénéfices

- Intégrable dans des systèmes externes (front, automate)
- Dockerisable pour mise en production légère
- Compatible avec tests e2e ou appel par module Java

---

## 🛰 Phase 5 – Airflow orchestration

### Objectif : automatiser les traitements de matching

- 1 DAG par provider (créé dynamiquement)
- 1 tâche par scope (pays)
- Appel de `run_matching(provider, scope)` dans chaque tâche

### Bénéfices

- Monitoring granulaire
- Redémarrage partiel possible
- Intégration dans la chaîne de traitement DataOps

---

## 🔁 Phase 6 – Supervision multi-projet (interopérabilité Java)

### Objectif : superviser aussi les modules externes qui appellent le moteur

- Airflow supervise :
  - tâche 1 : pipeline Java (via BashOperator)
  - tâche 2 : moteur Python (via PythonOperator)
- Le moteur reste traçable même s’il est déclenché depuis Java

---

## 🛠 Phase 7 – Génération automatique de provider

### Objectif : standardiser l’ajout d’un nouveau provider

- Script `add_provider.sh` :
  - met à jour le YAML
  - génère un DAG pré-rempli
  - crée les entrées nécessaires au moteur

---

## 🧪 Bénéfices pour les tests

- Chaque module `src/core/` est testable individuellement
- `ProviderRunner` est injecté avec des paramètres de test
- YAML peut être mocké ou surchargé pour simuler n’importe quel provider
- API testable séparément avec FastAPI test client
- Airflow ne bloque pas les tests (le moteur ne dépend pas d’Airflow)

---

## 🔭 Étapes futures (vision)

| Élément | Objectif |
|--------|----------|
| GraphES + LLM pour matching intelligent | Matching plus fin, intelligent, explicable |
| Indexation vectorielle + scoring contextuel | Support scalable pour des millions d’entités |
| CI/CD & Dockerisation complète | Déploiement standardisé, versioning piloté |
| Monitoring via Prometheus/Grafana | Observabilité avancée, suivi opérationnel |
