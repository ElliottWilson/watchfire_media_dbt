WITH entry_time AS 
(SELECT 
*,
MAX(time_of_entry) OVER (PARTITION BY asin_region) AS max_time_of_entry,
FROM {{ source('raw', 'fb_ads_asin_mapping') }})


SELECT 
* EXCEPT (campaign_id, time_of_entry, cost), 
CAST(campaign_id AS INT64) AS campaign_id, 
CAST(time_of_entry AS DATETIME) AS time_of_entry, 
CAST(cost AS FLOAT64) AS cost
FROM entry_time

WHERE time_of_entry = max_time_of_entry


