SELECT 
    marketplace,
    region
FROM {{ source('raw', 'regions') }}
WHERE marketplace != 'Marketplace'