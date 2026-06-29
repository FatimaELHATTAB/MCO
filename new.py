Bonjour,

Avant mon départ en congés, je vous partage un point d’avancement sur les travaux réalisés autour des pipelines CI et des analyses de sécurité.

Nous avons réussi à mettre en place l’exécution des pipelines via Loop, ce qui permet désormais de générer les rapports Sonatype, Sonar et Fortify directement dans le processus CI. Cette mise en place a demandé plusieurs ajustements, notamment pour aligner la configuration des pipelines avec les exigences attendues sur les rapports et les images générées.

Ce travail a également permis de corriger le problème identifié sur les labels des images Docker pour la partie ULM moteur. Les images produites sont désormais correctement renseignées, ce qui permet de lever ce point de blocage.

Côté analyse sécurité, le rapport Fortify ne remonte aucune anomalie : 0 issue détectée.

Concernant Sonatype, deux dépendances présentent encore des alertes avec un score supérieur à 8 :

* APScheduler
* Pandas

Une analyse détaillée a été réalisée sur ces deux alertes. Elles concernent des fonctionnalités des bibliothèques qui ne sont pas utilisées dans notre implémentation actuelle. Le risque est donc limité dans notre contexte d’usage.

Des waivers ont été créés pour couvrir ces deux points :

* Waiver APScheduler : [à compléter]
* Waiver Pandas : [à compléter]

La dernière image à déployer afin que l’ensemble des contrôles passe correctement est la suivante :

* Image à déployer : [à compléter]

Liens utiles :

* Pipeline CI : [à compléter]
* Rapport Sonatype : [à compléter]
* Rapport Sonar : [à compléter]
* Rapport Fortify : [à compléter]

En synthèse, la génération des rapports via Loop est désormais opérationnelle, le problème des labels d’images a été corrigé, Fortify ne remonte aucune issue, et les alertes Sonatype restantes ont été analysées et couvertes par des waivers.

Cordialement,
Tima





----------------------------------------



  Hi all,

As the QUAL PV preparation is planned for **3 July**, here is a status update on the remaining items required for validation.

We have not been able to perform the non-regression tests as initially expected, as we still do not have ISO production data available in the qualification environment.

Therefore, we are proceeding as follows:

1. **All rules except the fuzzy rule**
   These checks have already been performed on three providers. The objective is to compare the production code with the QUAL code, using QUAL data, with the expectation of obtaining **100% identical ISO results**.
   Hajar will now consolidate this analysis on **Bloomberg**, which is the most important provider.

2. **Fuzzy rule**
   A first analysis has already been carried out. We observed that there is no significant gap compared to the previous results, especially since some corrections have been made.
   The remaining discrepancies can now be explained either by initial data differences or by the technical optimizations we introduced to improve the program’s performance.
   Paolo will perform this analysis across all providers and document the explanations for the identified gaps.

3. **Qualification run**
   So far, runs have been performed on the qualification databases, but from the local environment.
   Before final validation, Hajar will need to deploy the latest version and perform a full run directly on the QUAL environment.

Once these three points are completed and validated, we should be able to proceed with the QUAL PV preparation as planned.

Best regards,
Fatima



     ------------------



     APScheduler is required by the application to schedule internal background jobs. 
The dependency is declared in the project runtime dependencies and is used only inside a controlled backend execution context.

The identified vulnerability is currently accepted temporarily because:
- the application does not expose APScheduler directly to external users;
- job definitions are controlled by the application code and are not provided by user input;
- there is no public endpoint allowing arbitrary job creation, modification, or execution;
- the container runs in a restricted environment with limited network and system permissions.

The dependency version has been reviewed and pinned/managed through uv.lock to ensure reproducible builds. 
We will continue to monitor the vulnerability and upgrade APScheduler as soon as a fixed compatible version is available.
