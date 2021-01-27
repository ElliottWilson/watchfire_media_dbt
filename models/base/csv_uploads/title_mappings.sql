SELECT 
* EXCEPT (bigquery_name), 
LOWER(asin_region) AS biquery_name
FROM {{ source('raw', 'title_mappings')}}