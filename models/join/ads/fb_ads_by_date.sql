SELECT 
fb_ads_asin_mapping.ASIN_region, 
fbm_ad.ad_id, 
fbm_ad.ob_date, 
fbm_ad.spend

FROM {{ ref('fb_ads_asin_mapping') }} 
LEFT JOIN {{ ref('fbm_ad') }} AS fbm_ad
ON fb_ads_asin_mapping.campaign_id = fbm_ad.campaign_id

WHERE ASIN_region NOT IN ('.US', '.')

GROUP BY 1, 2, 3, 4

