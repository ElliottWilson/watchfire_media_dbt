WITH sales_with_region AS(
SELECT 
(ASIN_ISBN ||'.'|| region.region) AS ASIN_region,
region.region AS region,
combined_sales.*,
(combined_sales.month_year||'-'||region.currency) month_year_currency_code
FROM {{ ref('combined_sales') }} AS combined_sales 
JOIN {{ ref('region_and_currency') }} AS region
ON combined_sales.marketplace = region.marketplace)

SELECT
sales_with_region.*,
currency_usd AS conversion_to_USD_in_month,
royalty * currency_usd AS royalty_usd
FROM sales_with_region
LEFT JOIN {{ ref('currencies') }} AS currencies
ON sales_with_region.month_year_currency_code = currencies.month_year_currency