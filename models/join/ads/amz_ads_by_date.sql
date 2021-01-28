SELECT 
ob_date,
(asin ||'.'|| region_and_currency.region) AS ASIN_region,
ad_id,
asin AS ASIN_ISBN,
campaign_id,
campaign_name,
clicks,
cost, 
amz_ads.currency, 
impressions
FROM 
{{ ref('amzadvertising_sp_product_ads') }} AS amz_ads
LEFT JOIN {{ ref('region_and_currency') }} AS region_and_currency
ON amz_ads.currency = region_and_currency.currency
WHERE asin IS NOT NULL
