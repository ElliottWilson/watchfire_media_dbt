WITH region_dates AS (
SELECT ASIN_region, date, region, ASIN_code AS ASIN_ISBN FROM {{ ref('kenp_sales') }} 
UNION DISTINCT 
SELECT ASIN_region, royalty_date, region, ASIN_ISBN FROM {{ ref('book_sales') }}),

book_sales_by_day AS (
SELECT 
ASIN_region, 
royalty_date AS date, 
IFNULL(SUM(units_sold),0) AS total_units_sold, 
IFNULL(SUM(units_refunded),0) AS total_units_refunded, 
IFNULL(SUM(net_units_sold),0) AS net_units_sold,
IFNULL(AVG(avg_list_price_without_tax),0) AS avg_list_price_without_tax,
IFNULL(AVG(avg_offer_price_without_tax),0) AS avg_offer_price_without_tax,	
IFNULL(AVG(avg_delivery_Manufacturing_cost),0) AS avg_delivery_Manufacturing_cost,
IFNULL(SUM(royalty_usd),0) AS books_sale_royalty_usd,
IFNULL(SUM(royalty),0) AS books_sale_royalty_native_currency,
FROM {{ ref('book_sales') }}
GROUP BY 1,2),

kenp_sales_by_day AS (
SELECT 
ASIN_region, 
date,
IFNULL(SUM(kindle_edition_normalized_pages),0) AS total_pages_read,
IFNULL(AVG(kindle_edition_normalized_pages),0)  AS avg_pages_read,
IFNULL(SUM(royalty_native_currency),0) AS kenp_royalty_native_currency,
IFNULL(SUM(royalty_usd),0) AS kenp_royalty_usd
FROM {{ ref('kenp_sales') }}
GROUP BY 1,2)

SELECT 
region_dates.*,
IFNULL(book_sales_by_day.total_units_sold,0) AS total_units_sold,
IFNULL(book_sales_by_day.total_units_refunded,0) AS total_units_refunded, 
IFNULL(book_sales_by_day.net_units_sold,0) AS net_units_sold,
IFNULL(book_sales_by_day.avg_list_price_without_tax,0) AS avg_list_price_without_tax,
IFNULL(book_sales_by_day.avg_offer_price_without_tax,0) AS avg_offer_price_without_tax,	
IFNULL(book_sales_by_day.avg_delivery_Manufacturing_cost,0) avg_delivery_Manufacturing_cost,
IFNULL(book_sales_by_day.books_sale_royalty_usd,0) AS books_sale_royalty_usd,
IFNULL(book_sales_by_day.books_sale_royalty_native_currency,0) books_sale_royalty_native_currency,
IFNULL(kenp_sales_by_day.total_pages_read,0) AS total_pages_read,
IFNULL(kenp_sales_by_day.avg_pages_read,0) AS avg_pages_read,
IFNULL(kenp_sales_by_day.kenp_royalty_native_currency,0) AS kenp_royalty_native_currency,
IFNULL(kenp_sales_by_day.kenp_royalty_usd,0) AS kenp_royalty_usd,
IFNULL(kenp_sales_by_day.kenp_royalty_native_currency,0) + IFNULL(book_sales_by_day.books_sale_royalty_native_currency,0) AS total_royalty_native_currency,
IFNULL(kenp_sales_by_day.kenp_royalty_usd,0) + IFNUll(book_sales_by_day.books_sale_royalty_usd,0) AS total_royalty_usd
FROM region_dates
LEFT JOIN 
book_sales_by_day ON
region_dates.ASIN_region = book_sales_by_day.ASIN_region 
AND region_dates.date = book_sales_by_day.date
LEFT JOIN 
kenp_sales_by_day ON
region_dates.ASIN_region = kenp_sales_by_day.ASIN_region 
AND region_dates.date = kenp_sales_by_day.date