Objectif du projet
Mettre en place un dashboard pour piloter le matching entre RMPM et les providers de données (Bloomberg, MSCI, CDP, Sustainalytics, Orbis).
Ce dashboard permettra de suivre les taux de matching, identifier les zones critiques, et faciliter la prise de décision.

Le projet est réalisé en collaboration avec les équipes de Lisbonne :

Notre rôle : structurer et préparer les données.
Lisbonne : intégration et développement du dashboard Power BI.
Livrable attendu
Dashboard interactif avec filtres sur :

Date
Provider
Entité
Indicateurs clés :

Nombre et pourcentage de records appariés
Nombre et pourcentage de records non appariés
Suivi de l’évolution des taux de matching et identification des actions à mettre en place.

Enjeux
Piloter la qualité du matching entre RMPM et les providers.
Identifier rapidement les écarts ou les faiblesses de couverture.
Faciliter les échanges avec les providers pour améliorer la donnée.
Section Incidents : Montants d’engagement et RWA
Le dashboard a également vocation à intégrer une section de suivi des incidents liés aux montants d’engagement et RWA.

Aujourd’hui, ces données ne sont pas accessibles pour être intégrées directement au dashboard.
Cependant, l’objectif est de :

Tracer les incidents où un mauvais matching impacte les montants d’engagement ou les RWA.
Suivre ces incidents dans le temps, avec un statut, une date, et des actions associées.
Cette section permettra, dès que les données seront disponibles, de :


Performance
Moteur de traitement basé sur Polars (Rust), utilisé pour l’ensemble des traitements de matching en mémoire.

Exécution stable sur des volumétries de plusieurs millions de lignes (ex : données Bloomberg), sans problème de performance constaté à ce jour.

Consommation mémoire maîtrisée :

Jamais au-delà de 7 Go de RAM consommée en exécution (70 Go actuellement alloués sur le cluster, possibilité d’allocation jusqu’à 128 Go si besoin).

Benchmarks internes : nette amélioration des temps de traitement par rapport aux solutions précédentes (Pandas, Spark).

Maîtrise technique
Solution intégralement maîtrisée en interne (architecture, code, logique de matching).

Capacité d’évolution rapide (ajout de règles, adaptation aux nouveaux besoins métier).

Qualité fonctionnelle
Résultats de matching validés par les utilisateurs finaux sur les campagnes en production.

Qualité conforme aux besoins métier actuels.

Évolutivité / agilité
Intégration de nouveaux providers opérationnelle en quelques jours.

Architecture supportant une montée en charge (PODs, scale-out si besoin).

Points d’amélioration identifiés
Le fuzzy matching est aujourd’hui réalisé via l’API MatchMyData (héritage de la solution initiale Dataiku).

Une évolution vers un fuzzy matching directement intégré dans le moteur est en cours, ce qui permettra d’améliorer encore la performance et l’autonomie de la solution.




----------------

Points d’amélioration identifiés
Historique et positionnement du fuzzy matching :

Initialement, l’ensemble du matching (core + fuzzy) était réalisé sur Dataiku :

le core via des jointures/règles dans l'outil no-code,

le fuzzy via l’interface manuelle de MatchMyData.

Lors de la migration vers Polars, le core du matching a été repris en interne (performant et maîtrisé), et l’interface MatchMyData a été remplacée par un appel automatisé à leur API, pour conserver la logique fuzzy existante.

Limitation actuelle :

L’usage de l’API MatchMyData nécessite de transmettre un fichier de grande taille (> 2 millions de lignes), ce qui est devenu une contrainte lourde en production (limitations techniques, complexité opérationnelle, lenteur du traitement).

Cette dépendance freine la scalabilité et l’automatisation complète du process.

Évolution prévue :

L’objectif est de supprimer l’usage de l’API MatchMyData et d’intégrer un moteur de fuzzy matching directement dans la chaîne de traitement interne (Polars + librairies adaptées), pour :

lever la contrainte liée à l’envoi de fichiers massifs,

améliorer la performance et la fluidité du process,

disposer d’une solution entièrement maîtrisée et scalable.

