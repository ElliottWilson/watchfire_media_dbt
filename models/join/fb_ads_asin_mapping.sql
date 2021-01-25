WITH fb_ads_asin_mapping AS (SELECT * EXCEPT (campaign_id), 
CAST(campaign_id AS INT64) 
AS campaign_id FROM {{ source('raw', 'fb_ads_asin_mapping') }}) 

SELECT fbm_ad.*, fb_ads_asin_mapping.ASIN_region FROM fb_ads_asin_mapping

LEFT JOIN {{ ref('fbm_ad') }} AS fbm_ad
ON fb_ads_asin_mapping.campaign_id = fbm_ad.campaign_id
-- Needs joining on title_mappings 