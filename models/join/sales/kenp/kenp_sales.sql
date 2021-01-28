WITH kenp_region AS (
SELECT 
  date, 
  month, 
  author_name,
  ASIN_code, 
  kenp.marketplace, 
  kindle_edition_normalized_pages,
  (kenp.month ||'.'|| region.clean_region) AS month_region,
  region.clean_region
FROM {{ ref('KENP') }} AS kenp
JOIN {{ ref('region_and_currency') }} AS region
ON kenp.marketplace = region.marketplace)

SELECT 
  kenp_region.*,
  rate_native_currency_pg,
  rate_USD_pg
FROM kenp_region
LEFT JOIN {{ ref('kenp_rates') }} AS kenp_rates
ON kenp_region.month_region = kenp_rates.month_region