
INSERT INTO own_91109_svg_um.provider_data_normalized
SELECT * FROM (
    SELECT * FROM own_91109_svg_um.provider_data_normalized_old
    LIMIT 100000
) sub
ON CONFLICT (local_id, incorporation_country, flux_source) DO NOTHING;



SELECT 
    relname,
    n_live_tup AS estimation_lignes
FROM pg_stat_user_tables
WHERE relname LIKE 'provider_data_normalized%'
ORDER BY relname;


SELECT 
    relname,
    pg_size_pretty(pg_total_relation_size(relid)) AS taille_totale,
    pg_size_pretty(pg_relation_size(relid)) AS taille_data,
    pg_size_pretty(pg_indexes_size(relid)) AS taille_index
FROM pg_stat_user_tables
WHERE relname LIKE 'provider_data_normalized%'
ORDER BY pg_total_relation_size(relid) DESC;


SELECT pid, now() - query_start AS duration, state, left(query, 100) AS query
FROM pg_stat_activity
WHERE state = 'active';


INSERT INTO own_91109_svg_um.provider_data_normalized
SELECT * FROM own_91109_svg_um.provider_data_normalized_old
WHERE incorporation_country = 'FR'
ON CONFLICT (local_id, incorporation_country, flux_source) DO NOTHING;


-- Ça compte vraiment mais prend du temps
SELECT COUNT(*) FROM own_91109_svg_um.provider_data_normalized_fr;

-- Ou par partition pour être plus rapide
SELECT 
    COUNT(*) FROM own_91109_svg_um.provider_data_normalized_fr
UNION ALL
SELECT 
    COUNT(*) FROM own_91109_svg_um.provider_data_normalized_other;
