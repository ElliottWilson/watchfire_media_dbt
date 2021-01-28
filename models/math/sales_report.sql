WITH region_dates AS (
SELECT ASIN_region, date FROM {{ ref('total_revenue_by_date') }} 
UNION DISTINCT 
SELECT ASIN_region, ob_date FROM {{ ref('all_ads_by_date') }})

SELECT 
title_mappings.asin_region,
title_mappings.asin_isbn,
region_dates.date,
title_mappings.title,
title_mappings.region,
title_mappings.author,
title_mappings.series,
title_mappings.book_num,
total_units_sold,
total_units_refunded, 
net_units_sold,
avg_list_price_without_tax,
avg_offer_price_without_tax,	
avg_delivery_Manufacturing_cost,
books_sale_royalty_usd,
books_sale_royalty_native_currency,
total_pages_read,
avg_pages_read,
kenp_royalty_native_currency,
kenp_royalty_usd,
total_royalty_native_currency,
total_royalty_usd,
all_ads_by_date.total_cost_amz, 
all_ads_by_date.total_cost_fb,
all_ads_by_date.total_ad_spend,
total_royalty_usd - total_ad_spend AS net_profit
FROM region_dates
INNER JOIN {{ ref('title_mappings') }} AS title_mappings
ON region_dates.ASIN_region = title_mappings.asin_region
LEFT JOIN {{ ref('all_ads_by_date') }} AS all_ads_by_date
ON all_ads_by_date.asin_region = region_dates.asin_region
AND region_dates.date = all_ads_by_date.ob_date
LEFT JOIN {{ ref('total_revenue_by_date') }} AS total_revenue_by_date
ON region_dates.asin_region = total_revenue_by_date.asin_region
AND region_dates.date = total_revenue_by_date.date

