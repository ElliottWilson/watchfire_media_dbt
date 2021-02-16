WITH raw_data AS (
SELECT 
* EXCEPT (bigquery_name), 
LOWER(asin_region) AS biquery_name,
MAX(time_of_entry) OVER (PARTITION BY asin_region) AS max_time_of_entry
FROM {{ source('raw', 'title_mappings')}})

SELECT 
* 
FROM raw_data
WHERE max_time_of_entry = time_of_entry