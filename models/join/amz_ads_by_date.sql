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
ob_date
FROM 
    {{ ref('amzadvertising_sp_product_ads') }} AS amz_ads
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
impressions
FROM ad 
LEFT JOIN portfolio
    ON ad.ASIN_ISBN = portfolio.ASIN_ISBN
ORDER BY ob_date, title
