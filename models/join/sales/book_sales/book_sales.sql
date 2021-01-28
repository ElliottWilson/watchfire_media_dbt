WITH sales_with_region AS(
SELECT 
(ASIN_ISBN ||'.'|| region.region) AS ASIN_region,
combined_sales.*,
(combined_sales.month||'.'||region.currency) month_currency_code
FROM {{ ref('combined_sales') }} AS combined_sales 
JOIN {{ ref('region_and_currency') }} AS region
ON combined_sales.marketplace = region.marketplace)

SELECT
sales_with_region.*,
conversion_to_USD AS conversion_to_USD_in_month
FROM sales_with_region
LEFT JOIN {{ ref('currencies') }} AS currencies
ON sales_with_region.month_currency_code = currencies.month_currency_code