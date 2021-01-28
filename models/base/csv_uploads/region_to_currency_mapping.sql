SELECT 
* 
FROM {{ source('raw', 'region_to_currency_mapping') }}
WHERE region != 'Region'