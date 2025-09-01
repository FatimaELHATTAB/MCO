---

# 📊 Projet TVA – Processus Tactique & Cible

## 1. Réception des données

**Tactique**

* Scope CRDS reçu par mail, déposé sur COS qualif.
* Tests mi-octobre.
  **Cible**
* Données via table certifiée (golden source).
  ➡️ Pas d’action à faire.

---

## 2. Calcul algorithmique

**Tactique**

* Algo défini & implémenté.
* Question : déploiement **avec le matching** ou en job séparé ?
  **Cible**
* Exécution prévue dans le **backend UM**.
  ➡️ Décider niveau de déploiement + intégrer dans DAG.

---

## 3. Matching (Orbis & Reuters)

**Tactique**

* Orbis : règles déjà définies.
* Reuters : règles à définir.
* Fichiers Dexter attendus le 10/09 pour tests.
  **Cible**
* Matching industrialisé dans pipeline certifié.
  ➡️ Actions : intégrer fichiers, définir règles Reuters, produire `tva_candidate` avec score.

---

## 4. Priorisation des sources

**Tactique**

* Priorisation Orbis > Reuters > Algo (exceptions pays ex. Pologne).
  **Cible**
* Doctrine codifiée dans table de poids/règles.
  ➡️ Valider doctrine et implémenter sélecteur `tva_selected`.

---

## 5. Qualification (VIES/API)

**Tactique**

* Appel API VIES pour valider numéros.
  ⚠️ Contacter **Vincent Niclas** pour whitelisting.
  **Cible**
* Qualification intégrée au pipeline certifié avec cache & retry.
  ➡️ Configurer connecteur API + cache (7–30j).

---

## 6. Génération & distribution

**Tactique**

* Résultats envoyés **par mail** à CRDS depuis qualif.
  **Cible**
* Mise en place d’une **API de diffusion** (UMD-API/Denodo).
  ➡️ Actions : automatiser export mail (tactique) + concevoir API (cible).

---

## Goldenisation (Cible)

* Survivorship : API nationale > VIES > Orbis > Reuters > Algo.
* Clés : `ID_RMPM` ou {raison sociale + pays}.
* Historisation : SCD2, traçabilité complète.
  ➡️ Rédiger doctrine & implémenter vue golden.

---

## Décisions à trancher

* Déploiement calcul en tactique (**avec matching** ou séparé).
* Cible : exécuter calcul dans **backend UM**.
* Seuils fuzzy Reuters & ordre Orbis/Reuters.
* TTL cache VIES (7, 14 ou 30 jours).
* Contrat API cible (format, filtres, auth).
