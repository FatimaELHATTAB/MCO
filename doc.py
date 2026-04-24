DO $$ DECLARE
    r RECORD;
BEGIN
    -- Supprimer les tables
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'mon_schema') LOOP
        EXECUTE 'DROP TABLE mon_schema.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;

    -- Supprimer les séquences
    FOR r IN (SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'mon_schema') LOOP
        EXECUTE 'DROP SEQUENCE mon_schema.' || quote_ident(r.sequence_name) || ' CASCADE';
    END LOOP;

    -- Supprimer les vues
    FOR r IN (SELECT table_name FROM information_schema.views WHERE table_schema = 'mon_schema') LOOP
        EXECUTE 'DROP VIEW mon_schema.' || quote_ident(r.table_name) || ' CASCADE';
    END LOOP;
END $$;
