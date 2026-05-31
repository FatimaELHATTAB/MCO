
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
