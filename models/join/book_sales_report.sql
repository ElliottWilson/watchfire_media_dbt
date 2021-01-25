WITH amz_and_fb_dates_ASIN AS (
SELECT amz_ads.ob_date AS ob_date, amz_ads.ASIN_region AS ASIN_region
FROM {{ ref('amz_ads_by_date') }} AS amz_ads

UNION DISTINCT 

SELECT fb_ads.ob_date, fb_ads.ASIN_region
FROM {{ ref('fb_ads_asin_mapping') }} AS fb_ads)

SELECT amz_and_fb_dates_ASIN.ob_date, amz_ads.title, amz_ads.cost, fb_ads.spend

FROM {{ ref('amz_ads_by_date') }} AS amz_ads

LEFT JOIN amz_and_fb_dates_ASIN
ON amz_ads.ASIN_region = amz_and_fb_dates_ASIN.ASIN_region 
AND amz_ads.ob_date = amz_and_fb_dates_ASIN.ob_date