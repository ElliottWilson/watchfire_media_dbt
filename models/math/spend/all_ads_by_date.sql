WITH amz_and_fb_dates_ASIN AS (
SELECT 
CAST (amz_ads.ob_date AS DATE) AS ob_date, 
amz_ads.ASIN_region AS ASIN_region
FROM {{ ref('amz_ads_by_date') }} AS amz_ads
UNION DISTINCT 
SELECT 
CAST (fb_ads.ob_date AS DATE) AS ob_date,
fb_ads.ASIN_region
FROM {{ ref('fb_ads_by_date') }} AS fb_ads
),

fb_ads_spend AS (
SELECT 
ob_date, 
ASIN_region, 
IFNULL(CAST(SUM(spend) AS numeric),0) AS total_spend
FROM {{ ref('fb_ads_by_date') }}
GROUP BY ob_date, ASIN_region
),

amz_ads_spend AS (
SELECT 
ob_date, 
ASIN_region,
IFNULL(CAST(SUM(cost) AS numeric),0) AS total_cost
FROM {{ ref('amz_ads_by_date') }}
GROUP BY ob_date, ASIN_region
)

SELECT 
amz_and_fb_dates_ASIN.ob_date,
amz_and_fb_dates_ASIN.ASIN_region,
IFNULL(CAST(amz_ads_spend.total_cost AS numeric),0) AS total_cost_amz,
IFNULL(CAST(fb_ads_spend.total_spend AS numeric),0) AS total_cost_fb,
IFNULL(CAST(amz_ads_spend.total_cost AS numeric),0) + IFNULL(CAST(fb_ads_spend.total_spend AS numeric),0) AS total_ad_spend
FROM amz_and_fb_dates_ASIN
LEFT JOIN amz_ads_spend
ON amz_and_fb_dates_ASIN.ASIN_region = amz_ads_spend.ASIN_region
AND amz_and_fb_dates_ASIN.ob_date = amz_ads_spend.ob_date
LEFT JOIN fb_ads_spend 
ON amz_and_fb_dates_ASIN.ASIN_region = fb_ads_spend.ASIN_region
AND amz_and_fb_dates_ASIN.ob_date = fb_ads_spend.ob_date
