BEGIN;

WITH batch AS MATERIALIZED (
    SELECT t.id, t.tpn_id, t.created_at
    FROM tax AS t
    WHERE t.tax_id_status = 'VIES_ERROR'
      AND t.closed_at IS NULL
      AND t.flux_execution_id <> 0
      AND t.created_by IS NOT NULL
      AND t.created_by NOT LIKE '%|TVA_OLD_FLUX=%'
      -- Ne pas préparer un autre batch avant le nettoyage.
      AND NOT EXISTS (
          SELECT 1
          FROM tax
          WHERE created_by LIKE '%|TVA_OLD_FLUX=%'
      )
    ORDER BY t.id
    LIMIT 30000
    FOR UPDATE OF t
),
choix AS MATERIALIZED (
    SELECT
        b.id AS erreur_id,
        p.id AS precedent_id,
        COALESCE(p.id, b.id) AS cible_id
    FROM batch AS b
    LEFT JOIN LATERAL (
        SELECT h.id
        FROM tax AS h
        WHERE h.tpn_id = b.tpn_id
          AND h.closed_at IS NOT NULL
          AND h.closed_by IS DISTINCT FROM 'CIB_RETRY'
          AND (h.created_at, h.id) < (b.created_at, b.id)
        ORDER BY h.created_at DESC, h.id DESC
        LIMIT 1
        FOR UPDATE OF h
    ) AS p ON TRUE
),
eligibles AS MATERIALIZED (
    SELECT c.*
    FROM choix AS c
    JOIN tax AS t ON t.id = c.cible_id
    WHERE t.created_by IS NOT NULL
      AND t.flux_execution_id <> 0
      AND t.created_by NOT LIKE '%|TVA_OLD_FLUX=%'
),
fermeture AS (
    UPDATE tax AS t
    SET closed_at = CURRENT_TIMESTAMP,
        closed_by = 'CIB_RETRY'
    FROM eligibles AS e
    WHERE t.id = e.erreur_id
      AND e.precedent_id IS NOT NULL
    RETURNING t.id
),
preparation AS (
    UPDATE tax AS t
    SET closed_at = NULL,
        closed_by = NULL,
        created_by = t.created_by
                     || '|TVA_OLD_FLUX='
                     || t.flux_execution_id::text,
        flux_execution_id = 0
    FROM eligibles AS e
    WHERE t.id = e.cible_id
      AND (
          e.precedent_id IS NULL
          OR EXISTS (
              SELECT 1
              FROM fermeture AS f
              WHERE f.id = e.erreur_id
          )
      )
    RETURNING t.id
)
SELECT COUNT(*) AS lignes_preparees
FROM preparation;

COMMIT;

-- Déclencher ensuite la notification TVA pour le flux 0.




BEGIN;

-- Bloquer la restauration si un TPN a plusieurs flux sauvegardés.
DO $$
BEGIN
    IF EXISTS (
        SELECT tpn_id
        FROM tax
        WHERE created_by ~ '[|]TVA_OLD_FLUX=[0-9]+$'
        GROUP BY tpn_id
        HAVING COUNT(DISTINCT substring(
            created_by FROM '[|]TVA_OLD_FLUX=([0-9]+)$'
        )::bigint) > 1
    ) THEN
        RAISE EXCEPTION 'Plusieurs flux sauvegardés pour un même TPN';
    END IF;
END $$;

-- Inclut les lignes fermées et les nouvelles lignes à 0.
WITH sauvegarde AS (
    SELECT DISTINCT
        tpn_id,
        substring(
            created_by FROM '[|]TVA_OLD_FLUX=([0-9]+)$'
        )::bigint AS ancien_flux
    FROM tax
    WHERE created_by ~ '[|]TVA_OLD_FLUX=[0-9]+$'
)
UPDATE tax AS t
SET flux_execution_id = s.ancien_flux
FROM sauvegarde AS s
WHERE t.tpn_id = s.tpn_id
  AND t.flux_execution_id = 0;

UPDATE tax
SET created_by = regexp_replace(
    created_by,
    '[|]TVA_OLD_FLUX=[0-9]+$',
    ''
)
WHERE created_by ~ '[|]TVA_OLD_FLUX=[0-9]+$';

COMMIT;




Objectif
Relancer TVA par batches de 30 000 lignes VIES_ERROR maximum,
sans table de sauvegarde et sans supprimer l’historique.

Préparation
- Rechercher la dernière version fermée précédant VIES_ERROR,
  en excluant les lignes fermées par CIB_RETRY.
- Si elle existe : fermer VIES_ERROR avec closed_by = CIB_RETRY
  et rouvrir cette version précédente.
- Sinon : conserver VIES_ERROR active.
- Sauvegarder le flux de la ligne retenue dans created_by.
- Mettre son flux_execution_id à 0.
- Les cibles avec created_by NULL ou flux NULL/0 sont ignorées.

Exécution
- Déclencher la notification TVA pour le flux 0.
- Attendre la fin complète du traitement.
- Exécuter la restauration, puis passer au batch suivant.

Restauration
- Retrouver le flux dans created_by, même sur une ligne fermée.
- Restaurer les lignes du même TPN ayant un flux à 0.
- Retirer le suffixe de sauvegarde de created_by.

Conditions
- Une seule ligne active par TPN et aucun traitement concurrent
  sur les TPN concernés pendant l’opération.
- Les lignes à flux 0 de ces TPN doivent appartenir à cette relance.
- created_by doit pouvoir contenir le suffixe ajouté.
- TVA doit utiliser la ligne active et non la dernière ligne créée.
- Si business_validity_to définit aussi la clôture, adapter le script.
- Une ligne restée VIES_ERROR peut être reprise au batch suivant :
  LIMIT seul ne garantit pas un passage unique.
