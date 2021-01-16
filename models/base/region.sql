WITH region AS (
SELECT 
Marketplace AS marketplace,
Region.Region AS region, 
BookBub_Region AS bookBub_region
FROM 
{{ source('raw', 'region') }}) 

SELECT region.marketplace, currencies.currency, region_to_currency_mapping.country_code

FROM region

LEFT JOIN {{ source('raw', 'region_to_currency_mapping') }} AS region_to_currency_mapping
ON region.region = region_to_currency_mapping.country_code

LEFT JOIN {{ source('raw', 'currencies') }} AS currencies
ON currencies.currency = region_to_currency_mapping.currency_code






