WITH kenp_region AS (
SELECT 
  date, 
  (ASIN_code ||'.'|| region.region) AS ASIN_region,
  region.region AS region,
  author_name,
  ASIN_code, 
  kenp.marketplace, 
  kindle_edition_normalized_pages,
  (kenp.month_year ||'-'|| region.clean_region) AS month_year_region,
  region.clean_region
FROM {{ ref('KENP') }} AS kenp
JOIN {{ ref('region_and_currency') }} AS region
ON kenp.marketplace = region.marketplace)

SELECT 
  kenp_region.*,
  rate_native_currency_page,
  rate_page,
  kindle_edition_normalized_pages * rate_native_currency_page AS royalty_native_currency,
  kindle_edition_normalized_pages * rate_page AS royalty_usd,
FROM kenp_region
LEFT JOIN {{ ref('kenp_rates') }} AS kenp_rates
ON kenp_region.month_year_region = kenp_rates.month_year_region

