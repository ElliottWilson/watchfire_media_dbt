WITH entry_time AS 

(SELECT *,
MAX(time_of_entry) OVER (PARTITION BY marketplace, region) AS max_time_of_entry
FROM {{ source('raw', 'regions') }})

SELECT 
marketplace,
region

FROM entry_time

WHERE time_of_entry = max_time_of_entry
