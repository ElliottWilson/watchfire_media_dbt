WITH entry_time AS 
(SELECT 
*,
MAX(time_of_entry) OVER (PARTITION BY author, bigquery_name) AS max_time_of_entry,
FROM {{ source('raw', 'data_feeds') }})

SELECT 
author, 
platform,
method, 
bigquery_name,
account,
status
FROM entry_time
WHERE time_of_entry = max_time_of_entry