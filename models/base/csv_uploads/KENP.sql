WITH kenp AS (SELECT
  Date AS date,
  EXTRACT(MONTH FROM date) AS month,
  Title AS title,
  Author_Name AS author_name,
  ASIN AS ASIN_code,
  Marketplace AS marketplace,
  Kindle_Edition_Normalized_Pages__KENP__Read_from_KU_and_KOLL AS kindle_edition_normalized_pages
FROM 
    {{ source('raw', 'KENP_read_csv') }}) 

SELECT 
  date, 
  month, 
  author_name,
  ASIN_code, 
  kenp.marketplace, 
  kindle_edition_normalized_pages,
  (kenp.month ||'.'|| region.clean_region) AS month_region,
  region.clean_region
FROM kenp 
JOIN {{ ref('region') }} AS region
ON kenp.marketplace = region.marketplace

