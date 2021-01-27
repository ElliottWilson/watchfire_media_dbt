SELECT * FROM {{ source('raw', 'region_to_currency_mapping_2') }}

WHERE region != 'Region'