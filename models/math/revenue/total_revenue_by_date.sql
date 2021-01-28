WITH region_dates AS (
SELECT ASIN_region, date, region, ASIN_code AS ASIN_ISBN FROM {{ ref('kenp_sales') }} 
UNION DISTINCT 
SELECT ASIN_region, royalty_date, region, ASIN_ISBN FROM {{ ref('book_sales') }}),

book_sales_by_day AS (
SELECT 
ASIN_region, 
royalty_date AS date, 
SUM(units_sold) AS total_units_sold, 
SUM(units_refunded) AS total_units_refunded, 
SUM(net_units_sold) AS net_units_sold,
AVG(avg_list_price_without_tax) AS avg_list_price_without_tax,
AVG(avg_offer_price_without_tax) AS avg_offer_price_without_tax,	
AVG(avg_delivery_Manufacturing_cost) AS avg_delivery_Manufacturing_cost,
SUM(royalty_usd) AS books_sale_royalty_usd,
SUM(royalty) AS books_sale_royalty_native_currency,
FROM {{ ref('book_sales') }}
GROUP BY 1,2),

kenp_sales_by_day AS (
SELECT 
ASIN_region, 
date,
SUM(kindle_edition_normalized_pages) AS total_pages_read,
AVG(kindle_edition_normalized_pages) AS avg_pages_read,
SUM(royalty_native_currency) AS kenp_royalty_native_currency,
SUM(royalty_usd) AS kenp_royalty_usd
FROM {{ ref('kenp_sales') }}
GROUP BY 1,2)

SELECT 
region_dates.*,
book_sales_by_day.total_units_sold,
book_sales_by_day.total_units_refunded, 
book_sales_by_day.net_units_sold,
book_sales_by_day.avg_list_price_without_tax,
book_sales_by_day.avg_offer_price_without_tax,	
book_sales_by_day.avg_delivery_Manufacturing_cost,
book_sales_by_day.books_sale_royalty_usd,
book_sales_by_day.books_sale_royalty_native_currency,
kenp_sales_by_day.total_pages_read,
kenp_sales_by_day.avg_pages_read,
kenp_sales_by_day.kenp_royalty_native_currency,
kenp_sales_by_day.kenp_royalty_usd,
kenp_sales_by_day.kenp_royalty_native_currency + book_sales_by_day.books_sale_royalty_native_currency AS total_royalty_native_currency,
kenp_sales_by_day.kenp_royalty_usd + book_sales_by_day.books_sale_royalty_usd AS total_royalty_usd
FROM region_dates
LEFT JOIN 
book_sales_by_day ON
region_dates.ASIN_region = book_sales_by_day.ASIN_region 
AND region_dates.date = book_sales_by_day.date
LEFT JOIN 
kenp_sales_by_day ON
region_dates.ASIN_region = kenp_sales_by_day.ASIN_region 
AND region_dates.date = kenp_sales_by_day.date