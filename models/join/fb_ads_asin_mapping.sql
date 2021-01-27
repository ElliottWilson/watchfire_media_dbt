WITH fb_ads_asin_mapping AS 
(SELECT 
* EXCEPT (campaign_id), 
CAST(campaign_id AS INT64) AS campaign_id 
FROM {{ source('raw', 'fb_ads_asin_mapping') }}) 

SELECT 
title_mappings.region, 
title, author, 
fbm_ad.*, 
fb_ads_asin_mapping.ASIN_region 
FROM fb_ads_asin_mapping
LEFT JOIN {{ ref('fbm_ad') }} AS fbm_ad
ON fb_ads_asin_mapping.campaign_id = fbm_ad.campaign_id
LEFT JOIN {{ ref('title_mappings') }} AS title_mappings
ON fb_ads_asin_mapping.ASIN_region = title_mappings.ASIN_region

