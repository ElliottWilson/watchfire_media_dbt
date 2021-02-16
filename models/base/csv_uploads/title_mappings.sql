WITH raw_data AS (
SELECT 
* EXCEPT (bigquery_name), 
LOWER(asin_region) AS biquery_name,
MAX(time_of_entry) OVER (PARTITION BY asin_region) AS latest_value
FROM {{ source('raw', 'title_mappings')}})

SELECT 
* 
FROM raw_data
WHERE latest_value = time_of_entry