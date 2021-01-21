WITH ad AS (
SELECT 
ad_id,
asin AS ASIN_ISBN,
campaign_id,
campaign_name,
clicks,
cost, 
currency, 
impressions,
ob_date, 
(asin ||'.'|| region.region) AS ASIN_region
FROM 
    {{ ref('amzadvertising_sp_product_ads') }} AS amz_ads
    LEFT JOIN {{ ref('region') }} AS region
    ON amz_ads.currency = region.currency_code
WHERE asin IS NOT NULL),

portfolio AS (
SELECT DISTINCT
title,
author_name,
ASIN_ISBN
FROM 
    {{ ref('portfolio') }} AS portfolio)

SELECT 
ob_date,
ad_id,
title,
author_name,
ad.ASIN_ISBN,
campaign_id,
campaign_name,
clicks,
cost, 
currency, 
impressions, 
ad.ASIN_region
FROM ad 
LEFT JOIN portfolio
    ON ad.ASIN_ISBN = portfolio.ASIN_ISBN

   
ORDER BY ob_date, title
