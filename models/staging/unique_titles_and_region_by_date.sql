{{ config(materialized='table')}}

WITH region_dates AS (
SELECT ASIN_region, date FROM {{ ref('total_revenue_by_date') }} 
UNION ALL 
SELECT ASIN_region, ob_date FROM {{ ref('all_ads_by_date') }}
)

SELECT DISTINCT
title.ASIN_region AS asin_region,
date
FROM {{ ref('title_mappings') }} AS title
LEFT JOIN
region_dates
ON region_dates.ASIN_region = title.asin_region