WITH entry_time AS 
(SELECT 
*,
MAX(time_of_entry) OVER (PARTITION BY region, currency) AS max_time_of_entry,
FROM {{ source('raw', 'region_to_currency_mapping') }})

SELECT 
* 
FROM entry_time

WHERE time_of_entry = max_time_of_entry
