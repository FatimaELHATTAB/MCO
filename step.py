Compte rendu – Avancement Fuzzy Matching
Choix techniques

Bibliothèque utilisée : nous n’avons pas retenu RecordLinkage (utilisé dans MatchMyData) car elle n’est pas compatible avec Polars.

Alternative mise en place : adoption de RapidFuzzy, qui permet un usage direct avec Polars et offre une meilleure intégration dans notre pipeline.

Prochaines étapes

Validation des résultats

Comparer la qualité et la précision obtenues avec RapidFuzzy par rapport à MatchMyData.

Confirmer la robustesse des règles de fuzzy matching.

Mesure des performances

Démontrer le gain de temps d’exécution avec RapidFuzzy par rapport à MatchMyData.

Documenter les benchmarks.

Qualité et supervision

Intégrer les tests unitaires dans SonarQube.

Configurer Sonar pour automatiser l’analyse de la qualité du code.

Hygiène du code et des dépôts

Nettoyer les repositories Git (branches, commits, fichiers obsolètes).

Standardiser les commits pour améliorer la lisibilité et la traçabilité.
