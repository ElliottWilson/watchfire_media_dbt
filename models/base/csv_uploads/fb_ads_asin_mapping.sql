SELECT 
* EXCEPT (campaign_id, time_of_entry, cost), 
CAST(campaign_id AS INT64) AS campaign_id, 
CAST(time_of_entry AS DATETIME) AS time_of_entry, 
CAST(cost AS FLOAT64) AS cost
FROM {{ source('raw', 'fb_ads_asin_mapping') }}

WHERE name != 'name'