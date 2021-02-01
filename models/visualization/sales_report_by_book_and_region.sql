{{ config(materialized='table')}}

WITH region_dates AS (
SELECT ASIN_region, date FROM {{ ref('total_revenue_by_date') }} 
UNION DISTINCT 
SELECT ASIN_region, ob_date FROM {{ ref('all_ads_by_date') }}
),

titles_and_region AS (
SELECT DISTINCT
title.ASIN_region AS asin_region,
date
FROM {{ ref('title_mappings') }} AS title
LEFT JOIN
region_dates
ON region_dates.ASIN_region = title.asin_region)


SELECT 
titles_and_region.asin_region,
title_mappings.asin_isbn,
titles_and_region.date,
title_mappings.title,
title_mappings.region,
title_mappings.author,
title_mappings.series,
title_mappings.book_num,
IFNULL(total_units_sold,0) AS total_units_sold,
IFNULL(total_units_refunded,0) AS total_units_refunded,
IFNULL(net_units_sold,0) AS net_units_sold,
IFNULL(avg_list_price_without_tax,0) AS avg_list_price_without_tax,
IFNULL(avg_offer_price_without_tax,0) AS avg_offer_price_without_tax,	
IFNULL(avg_delivery_Manufacturing_cost,0) AS avg_delivery_Manufacturing_cost,
IFNULL(books_sale_royalty_usd,0) AS books_sale_royalty_usd,
IFNULL(books_sale_royalty_native_currency,0) AS books_sale_royalty_native_currency,
IFNULL(total_pages_read,0) AS total_pages_read,
IFNULL(avg_pages_read,0) AS avg_pages_read,
IFNULL(kenp_royalty_native_currency,0) AS kenp_royalty_native_currency,
IFNULL(kenp_royalty_usd,0) AS kenp_royalty_usd,
IFNULL(total_royalty_native_currency,0) AS total_royalty_native_currency,
IFNULL(total_royalty_usd,0) AS total_royalty_usd,
IFNULL(all_ads_by_date.total_cost_amz,0) AS total_cost_amz, 
IFNULL(all_ads_by_date.total_cost_fb,0) AS total_cost_fb,
IFNULL(all_ads_by_date.total_ad_spend,0) AS total_ad_spend,
IFNULL(total_royalty_usd,0) - IFNULL(total_ad_spend,0) AS net_profit_usd
FROM titles_and_region
INNER JOIN {{ ref('title_mappings') }} AS title_mappings
ON titles_and_region.ASIN_region = title_mappings.asin_region
LEFT JOIN {{ ref('all_ads_by_date') }} AS all_ads_by_date
ON all_ads_by_date.asin_region = titles_and_region.asin_region
AND titles_and_region.date = all_ads_by_date.ob_date
LEFT JOIN {{ ref('total_revenue_by_date') }} AS total_revenue_by_date
ON titles_and_region.asin_region = total_revenue_by_date.asin_region
AND titles_and_region.date = total_revenue_by_date.date
ORDER BY date, asin_region

