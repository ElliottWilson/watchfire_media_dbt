{{ config(materialized='table')}}

SELECT
asin_isbn,
date,
title,
author,
series,
book_num,
SUM(total_units_sold) AS total_units_sold,
SUM(total_units_refunded) AS total_units_refunded,
SUM(net_units_sold) AS net_units_sold,
AVG(avg_list_price_without_tax) AS avg_list_price_without_tax,
AVG(avg_offer_price_without_tax) AS avg_offer_price_without_tax,
AVG(avg_delivery_Manufacturing_cost) AS avg_delivery_Manufacturing_cost,
SUM(books_sale_royalty_usd) AS books_sale_royalty_usd,
SUM(books_sale_royalty_native_currency) AS books_sale_royalty_native_currency,
SUM(total_pages_read) AS total_pages_read,
AVG(avg_pages_read) AS avg_pages_read,
SUM(kenp_royalty_native_currency) AS kenp_royalty_native_currency,
SUM(kenp_royalty_usd) AS kenp_royalty_usd,
SUM(total_royalty_native_currency) AS total_royalty_native_currency,
SUM(total_royalty_usd) AS total_royalty_usd,
SUM(total_cost_amz) AS total_cost_amz,
SUM(total_cost_fb) AS total_cost_fb,
SUM(total_ad_spend) AS total_ad_spend,
SUM(net_profit_usd) AS net_profit_usd,
FROM {{ ref('sales_report_by_book_and_region') }} 
GROUP BY 
asin_isbn,
date,
title,
author,
series,
book_num
ORDER BY date, asin_isbn