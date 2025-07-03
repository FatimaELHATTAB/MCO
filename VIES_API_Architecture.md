# Document d’Architecture – Validation TVA via VIES API

**Rédigé par** : Tima  
**Date** : Juillet 2025  
**Destinataires** : Équipe Architecture Sécurité  
**Objet** : Validation des numéros de TVA via l’API REST officielle VIES

---

## 1. Contexte

Actuellement, la validation des numéros de TVA intra-UE est effectuée manuellement via le site VIES de la Commission européenne, par traitement batch. Ce mode opératoire est :

- non traçable  
- non sécurisé  
- sujet à des erreurs humaines

Nous proposons une **automatisation maîtrisée** via l’API REST officielle VIES, dans un cadre sécurisé, sans exposition externe, et conforme aux standards du groupe.

---

## 2. Résumé de la solution technique

| Élément | Détail |
|--------|--------|
| **API consommée** | `https://ec.europa.eu/taxation_customs/vies/rest-api/check-vat-number` |
| **Type de requête** | POST en HTTPS (sans authentification, accès public contrôlé) |
| **Payload envoyé** | Numéro TVA + pays + identifiant déclarant |
| **Données sensibles ?** | **Non** – TVA publique, pas de données personnelles |
| **Nature de la donnée** | Publiquement vérifiable (service de l’UE) |
| **Mode d’appel** | Appel unitaire, depuis script interne Python |
| **Fréquence des appels** | À la demande (ex : onboarding fournisseur, traitement factures) |
| **Stockage** | Résultats logués localement, pas de réplication du service |

---

## 3. Sécurité et conformité

| Aspect | Dispositif mis en place |
|--------|--------------------------|
| **Authentification API** | Pas nécessaire – API publique de l’UE |
| **Connexion sortante** | Contrôle réseau : whitelist explicite de l’URL VIES |
| **Chiffrement** | HTTPS uniquement |
| **Passthrough data** | Aucune donnée sensible stockée ou exposée |
| **Risques identifiés** | - Risque de déni de service externe (mitigé par retry/backoff)<br>- Risque d'indisponibilité ponctuelle du service |
| **Logging** | Journalisation interne horodatée (request / response minimale) |
| **Auditabilité** | Appels horodatés et centralisés, traçabilité garantie |

---

## 4. Points de vigilance

- ❌ L’API **ne gère pas les vérifications par lot**
- 📶 **Pas de SLA** garanti par la Commission européenne
- ⚠️ Résultats variables selon les pays (nom/adresse parfois absents)

---

## 5. Exemple d’utilisation

### 🔸 Requête (payload JSON)

```json
{
  "countryCode": "FR",
  "vatNumber": "12345678901",
  "requesterMemberStateCode": "FR",
  "requesterNumber": "12345678901"
}
```

### 🔹 Réponse (exemple)

```json
{
  "countryCode": "FR",
  "vatNumber": "12345678901",
  "requestDate": "2025-07-02T15:32:10.000Z",
  "valid": true,
  "name": "Société XYZ",
  "address": "10 rue Exemple, Paris"
}
```

---

## 6. Ressources officielles

- 🌐 **Site officiel VIES** : [https://ec.europa.eu/taxation_customs/vies](https://ec.europa.eu/taxation_customs/vies)
- 📄 **Swagger OpenAPI** : [https://ec.europa.eu/assets/taxud/vow-information/swagger_publicVAT.yaml](https://ec.europa.eu/assets/taxud/vow-information/swagger_publicVAT.yaml)
- 💻 **Exemple PHP SDK** : [https://github.com/rocketfellows/vies-vat-validation-php-sdk-rest](https://github.com/rocketfellows/vies-vat-validation-php-sdk-rest)

---

✅ **Prêt pour soumission à validation sécurité**
