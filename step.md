🛠 Détail des étapes :
1️⃣ impACT envoie une requête API pour récupérer des documents selon des métadonnées spécifiques.
2️⃣ Vérification dans Document Indexation : on regarde si le document est déjà dans PDF Storage.
3️⃣ Si le document existe, on retourne directement son identifiant à impACT.
4️⃣ Si le document n’existe pas, on effectue une recherche dans MyDoc.
5️⃣ Si MyDoc a le document, on récupère l’identifiant du document.
6️⃣ On envoie tous les identifiants disponibles à impACT.
7️⃣ impACT sélectionne un document parmi ceux proposés.
8️⃣ Si impACT veut modifier un document, il envoie une requête avec l’ID + nouvelles métadonnées.
9️⃣ Mise à jour des métadonnées dans Document Indexation et propagation à PDF Storage / MyDoc.
🔟 Confirmation envoyée à impACT que les métadonnées ont bien été mises à jour.

