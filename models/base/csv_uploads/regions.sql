SELECT 
    marketplace,
    region
FROM {{ source('raw', 'regions_2') }}
WHERE marketplace != 'Marketplace'