WITH regions AS (
  SELECT 
    marketplace,
    regions.region AS region,
    CASE
    WHEN regions.region = 'DE' THEN 'EU'
    WHEN regions.region = 'ES' THEN 'EU'
    WHEN regions.region = 'FR' THEN 'EU'
    WHEN regions.region = 'IT' THEN 'EU'
    WHEN regions.region = 'US' THEN 'US'
    WHEN regions.region = 'NL' THEN 'EU'
    WHEN regions.region = 'CA' THEN 'CA'
    WHEN regions.region = 'AU' THEN 'AU'
    WHEN regions.region = 'IN' THEN 'IN'
    WHEN regions.region = 'MX' THEN 'MX'
    WHEN regions.region = 'BR' THEN 'BR'
    WHEN regions.region = 'UK' THEN 'UK'
    ELSE 'Region not defined'
END AS clean_region
FROM {{ ref('regions') }})

SELECT 
marketplace,
regions.region,
region_to_currency_mapping.currency,
regions.clean_region
FROM regions
LEFT JOIN {{ ref('region_to_currency_mapping') }} AS region_to_currency_mapping
ON regions.clean_region = region_to_currency_mapping.region
WHERE marketplace != 'Marketplace'

















