-- =============================================================================
-- MIGRATION : provider_data_normalized → table partitionnée par incorporation_country
-- Schéma : own_91109_svg_um
-- Volume : ~56M lignes | FR = ~23M lignes
-- =============================================================================

-- -----------------------------------------------------------------------------
-- ÉTAPE 1 : Renommer l'ancienne table
-- -----------------------------------------------------------------------------
ALTER TABLE own_91109_svg_um.provider_data_normalized
    RENAME TO provider_data_normalized_old;


-- -----------------------------------------------------------------------------
-- ÉTAPE 2 : Créer la nouvelle table partitionnée par LIST (incorporation_country)
-- Note : La PK (local_id, incorporation_country, flux_source) doit inclure
--        la clé de partition → incorporation_country est déjà dedans ✅
-- -----------------------------------------------------------------------------
CREATE TABLE own_91109_svg_um.provider_data_normalized
(
    local_id                 character varying(50) COLLATE pg_catalog."default" NOT NULL,
    flux_source              character varying(20) COLLATE pg_catalog."default" NOT NULL,
    flux_execution_id        bigint,
    company_name             text COLLATE pg_catalog."default",
    company_name_clean       text COLLATE pg_catalog."default",
    incorporation_country    character varying(2) COLLATE pg_catalog."default" NOT NULL,
    company_identifier_data  jsonb,
    tax_data                 jsonb,
    instrument_data          jsonb,
    isin_equity_data         jsonb,
    provider_data            jsonb,
    created_at               timestamp with time zone NOT NULL DEFAULT now(),

    CONSTRAINT provider_data_normalized_key
        PRIMARY KEY (local_id, incorporation_country, flux_source)
)
PARTITION BY LIST (incorporation_country)
WITH (autovacuum_enabled = FALSE)
TABLESPACE pg_default;


-- -----------------------------------------------------------------------------
-- ÉTAPE 3 : Créer les partitions
-- -----------------------------------------------------------------------------

-- Partition FR (23M lignes)
CREATE TABLE own_91109_svg_um.provider_data_normalized_fr
    PARTITION OF own_91109_svg_um.provider_data_normalized
    FOR VALUES IN ('FR')
    WITH (autovacuum_enabled = FALSE)
    TABLESPACE pg_default;

-- Partition US
CREATE TABLE own_91109_svg_um.provider_data_normalized_us
    PARTITION OF own_91109_svg_um.provider_data_normalized
    FOR VALUES IN ('US')
    WITH (autovacuum_enabled = FALSE)
    TABLESPACE pg_default;

-- Partition GB
CREATE TABLE own_91109_svg_um.provider_data_normalized_gb
    PARTITION OF own_91109_svg_um.provider_data_normalized
    FOR VALUES IN ('GB')
    WITH (autovacuum_enabled = FALSE)
    TABLESPACE pg_default;

-- Partition DE
CREATE TABLE own_91109_svg_um.provider_data_normalized_de
    PARTITION OF own_91109_svg_um.provider_data_normalized
    FOR VALUES IN ('DE')
    WITH (autovacuum_enabled = FALSE)
    TABLESPACE pg_default;

-- Partition DEFAULT (tous les autres pays)
CREATE TABLE own_91109_svg_um.provider_data_normalized_other
    PARTITION OF own_91109_svg_um.provider_data_normalized
    DEFAULT
    WITH (autovacuum_enabled = FALSE)
    TABLESPACE pg_default;

-- ⚠️  Ajouter d'autres partitions si tu as des pays avec > 5M lignes
-- ex: CREATE TABLE ... provider_data_normalized_es PARTITION OF ... FOR VALUES IN ('ES')


-- -----------------------------------------------------------------------------
-- ÉTAPE 4 : Droits (identiques à l'ancienne table)
-- -----------------------------------------------------------------------------
ALTER TABLE own_91109_svg_um.provider_data_normalized
    OWNER TO role_own_91109_svg_um;

REVOKE ALL ON TABLE own_91109_svg_um.provider_data_normalized
    FROM role_app_91109_svg_um;
REVOKE ALL ON TABLE own_91109_svg_um.provider_data_normalized
    FROM role_read_91109_svg_um;

GRANT DELETE, INSERT, SELECT, TRUNCATE, UPDATE
    ON TABLE own_91109_svg_um.provider_data_normalized
    TO role_app_91109_svg_um;

GRANT ALL ON TABLE own_91109_svg_um.provider_data_normalized
    TO role_own_91109_svg_um;

GRANT SELECT ON TABLE own_91109_svg_um.provider_data_normalized
    TO role_read_91109_svg_um;


-- -----------------------------------------------------------------------------
-- ÉTAPE 5 : Migration des données par batch de 500K lignes
-- On commence par FR (23M), puis le reste
-- Durée estimée : 20-45 min selon les ressources serveur
-- -----------------------------------------------------------------------------

-- 5a. Migrer FR en premier (partition la plus grosse)
DO $$
DECLARE
    batch_size   INT := 500000;
    offset_val   BIGINT := 0;
    rows_inserted INT;
    total        BIGINT := 0;
BEGIN
    RAISE NOTICE '[FR] Début migration FR...';
    LOOP
        INSERT INTO own_91109_svg_um.provider_data_normalized
        SELECT *
        FROM own_91109_svg_um.provider_data_normalized_old
        WHERE incorporation_country = 'FR'
        ORDER BY local_id, flux_source
        LIMIT batch_size OFFSET offset_val;

        GET DIAGNOSTICS rows_inserted = ROW_COUNT;
        EXIT WHEN rows_inserted = 0;

        total      := total + rows_inserted;
        offset_val := offset_val + batch_size;
        RAISE NOTICE '[FR] % lignes migrées...', total;
        PERFORM pg_sleep(0.05);
    END LOOP;
    RAISE NOTICE '[FR] ✅ Migration FR terminée : % lignes', total;
END $$;


-- 5b. Migrer tous les autres pays
DO $$
DECLARE
    batch_size   INT := 500000;
    offset_val   BIGINT := 0;
    rows_inserted INT;
    total        BIGINT := 0;
BEGIN
    RAISE NOTICE '[OTHER] Début migration hors FR...';
    LOOP
        INSERT INTO own_91109_svg_um.provider_data_normalized
        SELECT *
        FROM own_91109_svg_um.provider_data_normalized_old
        WHERE incorporation_country != 'FR'
        ORDER BY local_id, flux_source
        LIMIT batch_size OFFSET offset_val;

        GET DIAGNOSTICS rows_inserted = ROW_COUNT;
        EXIT WHEN rows_inserted = 0;

        total      := total + rows_inserted;
        offset_val := offset_val + batch_size;
        RAISE NOTICE '[OTHER] % lignes migrées...', total;
        PERFORM pg_sleep(0.05);
    END LOOP;
    RAISE NOTICE '[OTHER] ✅ Migration hors FR terminée : % lignes', total;
END $$;


-- -----------------------------------------------------------------------------
-- ÉTAPE 6 : Vérification des counts avant de créer les index
-- -----------------------------------------------------------------------------
SELECT
    'OLD'   AS source, COUNT(*) AS total FROM own_91109_svg_um.provider_data_normalized_old
UNION ALL
SELECT
    'NEW'   AS source, COUNT(*) AS total FROM own_91109_svg_um.provider_data_normalized
UNION ALL
SELECT
    'FR'    AS source, COUNT(*) AS total FROM own_91109_svg_um.provider_data_normalized_fr
UNION ALL
SELECT
    'OTHER' AS source, COUNT(*) AS total FROM own_91109_svg_um.provider_data_normalized_other;

-- ⚠️  Vérifier que OLD = NEW avant de continuer !


-- -----------------------------------------------------------------------------
-- ÉTAPE 7 : Créer les index (APRÈS migration pour être plus rapide)
-- -----------------------------------------------------------------------------

-- Index company_identifier_data (GIN) — global, hérité par les partitions
CREATE INDEX IF NOT EXISTS idx_company_identifier_data_provider_normalized
    ON own_91109_svg_um.provider_data_normalized USING gin
    (company_identifier_data)
    TABLESPACE pg_default;

-- Index flux_execution_id (btree)
CREATE INDEX IF NOT EXISTS idx_flux_execution_id_provider_normalized
    ON own_91109_svg_um.provider_data_normalized USING btree
    (flux_execution_id ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index composite group_by (local_id, flux_source, company_name, company_name_clean, incorporation_country)
CREATE INDEX IF NOT EXISTS idx_group_by_provider_normalized
    ON own_91109_svg_um.provider_data_normalized USING btree
    (
        local_id              COLLATE pg_catalog."default" ASC NULLS LAST,
        flux_source           COLLATE pg_catalog."default" ASC NULLS LAST,
        company_name          COLLATE pg_catalog."default" ASC NULLS LAST,
        company_name_clean    COLLATE pg_catalog."default" ASC NULLS LAST,
        incorporation_country COLLATE pg_catalog."default" ASC NULLS LAST
    )
    TABLESPACE pg_default;

-- Index incorporation_country simple (btree) — utile pour les petits pays
-- Pour FR (41%), le partition pruning remplace cet index
CREATE INDEX IF NOT EXISTS idx_incorporation_country_provider_normalized
    ON own_91109_svg_um.provider_data_normalized USING btree
    (incorporation_country COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index isin_equity_data (GIN)
CREATE INDEX IF NOT EXISTS idx_isin_equity_data_provider_normalized
    ON own_91109_svg_um.provider_data_normalized USING gin
    (isin_equity_data)
    TABLESPACE pg_default;

-- Index local_id (btree)
CREATE INDEX IF NOT EXISTS idx_local_id_provider_normalized
    ON own_91109_svg_um.provider_data_normalized USING btree
    (local_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index tax_data (GIN)
CREATE INDEX IF NOT EXISTS idx_tax_data_provider_normalized
    ON own_91109_svg_um.provider_data_normalized USING gin
    (tax_data)
    TABLESPACE pg_default;

-- -----------------------------------------------------------------------------
-- INDEX BONUS recommandés pour tes requêtes FR (cf. la requête SELECT visible)
-- -----------------------------------------------------------------------------

-- Composite très sélectif pour les requêtes avec filtre pays + local_id
CREATE INDEX IF NOT EXISTS idx_country_local_id_provider_normalized
    ON own_91109_svg_um.provider_data_normalized USING btree
    (incorporation_country, local_id)
    TABLESPACE pg_default;

-- Index partiel sur FR uniquement pour flux_execution_id (utile si tu filtres par batch)
CREATE INDEX IF NOT EXISTS idx_fr_flux_execution_id
    ON own_91109_svg_um.provider_data_normalized_fr USING btree
    (flux_execution_id ASC NULLS LAST)
    TABLESPACE pg_default;


-- -----------------------------------------------------------------------------
-- ÉTAPE 8 : Réactiver autovacuum sur les partitions après migration
-- -----------------------------------------------------------------------------
ALTER TABLE own_91109_svg_um.provider_data_normalized_fr
    SET (autovacuum_enabled = TRUE);
ALTER TABLE own_91109_svg_um.provider_data_normalized_us
    SET (autovacuum_enabled = TRUE);
ALTER TABLE own_91109_svg_um.provider_data_normalized_gb
    SET (autovacuum_enabled = TRUE);
ALTER TABLE own_91109_svg_um.provider_data_normalized_de
    SET (autovacuum_enabled = TRUE);
ALTER TABLE own_91109_svg_um.provider_data_normalized_other
    SET (autovacuum_enabled = TRUE);

-- ANALYZE pour mettre à jour les stats du planner
ANALYZE own_91109_svg_um.provider_data_normalized;


-- -----------------------------------------------------------------------------
-- ÉTAPE 9 : Supprimer l'ancienne table (UNIQUEMENT après validation complète)
-- -----------------------------------------------------------------------------
-- DROP TABLE own_91109_svg_um.provider_data_normalized_old;
-- ⚠️  Décommenter seulement après avoir validé que tout fonctionne en prod !


-- -----------------------------------------------------------------------------
-- ÉTAPE 10 : Vérifier que le partition pruning fonctionne
-- -----------------------------------------------------------------------------
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM own_91109_svg_um.provider_data_normalized
WHERE incorporation_country = 'FR'
LIMIT 100;
-- Tu dois voir : "Partitions: provider_data_normalized_fr" uniquement
-- Et PAS les autres partitions → pruning actif ✅
