WITH region AS (
  SELECT 
    Marketplace AS marketplace,
    Region.Region AS region,
    CASE
    WHEN region.region = 'DE' THEN 'EU'
    WHEN region.region = 'ES' THEN 'EU'
    WHEN region.region = 'FR' THEN 'EU'
    WHEN region.region = 'IT' THEN 'EU'
    WHEN region.region = 'US' THEN 'US'
    WHEN region.region = 'NL' THEN 'EU'
    WHEN region.region = 'CA' THEN 'CA'
    WHEN region.region = 'AU' THEN 'AU'
    WHEN region.region = 'IN' THEN 'IN'
    WHEN region.region = 'MX' THEN 'MX'
    WHEN region.region = 'BR' THEN 'BR'
    WHEN region.region = 'UK' THEN 'UK'
    ELSE 'Region not defined'
END AS clean_region
FROM {{ source('raw','region') }})

SELECT 
region.marketplace,
region.region,
region_to_currency_mapping.currency,
region.clean_region
FROM region
LEFT JOIN {{ ref('region_to_currency_mapping') }} AS region_to_currency_mapping
ON region.clean_region = region_to_currency_mapping.region
WHERE marketplace != 'Marketplace'

















