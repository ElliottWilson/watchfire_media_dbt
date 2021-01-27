WITH ad AS (
SELECT 
ad_id,
asin AS ASIN_ISBN,
campaign_id,
campaign_name,
clicks,
cost, 
region.currency, 
impressions,
ob_date, 
(asin ||'.'|| region.region) AS ASIN_region
FROM 
{{ ref('amzadvertising_sp_product_ads') }} AS amz_ads
LEFT JOIN {{ ref('region') }} AS region
ON amz_ads.currency = region.currency
WHERE asin IS NOT NULL),

title_mappings AS (
SELECT *
FROM 
{{ ref('title_mappings') }} AS title_mappings
)

SELECT 
ob_date,
ad_id,
title,
author,
ad.ASIN_ISBN,
campaign_id,
campaign_name,
clicks,
cost,
currency, 
impressions,
ad.ASIN_region
FROM ad 
LEFT JOIN title_mappings
ON ad.ASIN_region = title_mappings.ASIN_region
WHERE ASIN_region = 'B01G9FG15C.US'
