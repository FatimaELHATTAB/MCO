INSERT INTO own_91109_svg_um.provider_data_normalized
SELECT * FROM own_91109_svg_um.provider_data_normalized_old
ON CONFLICT (local_id, incorporation_country, flux_source) DO NOTHING
LIMIT 100000;
