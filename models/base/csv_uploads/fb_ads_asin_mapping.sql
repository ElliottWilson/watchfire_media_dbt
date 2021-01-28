SELECT 
* EXCEPT (campaign_id), 
CAST(campaign_id AS INT64) AS campaign_id 
FROM {{ source('raw', 'fb_ads_asin_mapping') }}
