SELECT 
fb_ads_asin_mapping.ASIN_region, 
fbm_ad.*
FROM {{ ref('fb_ads_asin_mapping') }} 
LEFT JOIN {{ ref('fbm_ad') }} AS fbm_ad
ON fb_ads_asin_mapping.campaign_id = fbm_ad.campaign_id

