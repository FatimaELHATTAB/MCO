\timing on

INSERT INTO own_91109_svg_um.provider_data_normalized
SELECT o.* 
FROM own_91109_svg_um.provider_data_normalized_old o
WHERE NOT EXISTS (
    SELECT 1 
    FROM own_91109_svg_um.provider_data_normalized n
    WHERE n.local_id = o.local_id
      AND n.incorporation_country = o.incorporation_country
      AND n.flux_source = o.flux_source
)
LIMIT 100000;
