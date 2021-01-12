SELECT
  Date AS date,
  Title AS title,
  Author_Name AS author_name,
  ASIN AS ASIN_code,
  Marketplace AS marketplace,
  Kindle_Edition_Normalized_Pages__KENP__Read_from_KU_and_KOLL AS kindle_edition_normalized_pages
FROM 
    {{ source('raw', 'KENP_read_csv') }}