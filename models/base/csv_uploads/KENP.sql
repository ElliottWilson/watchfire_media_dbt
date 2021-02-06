SELECT
  CAST(Date as DATE) AS date,
  EXTRACT(MONTH FROM CAST(Date as DATE)) AS month,
  Title AS title,
  Author_Name AS author_name,
  ASIN AS ASIN_code,
  Marketplace AS marketplace,
  IFNULL(CAST(Kindle_Edition_Normalized_Pages__KENP__Read_from_KU_and_KOLL AS INT64),0) AS kindle_edition_normalized_pages
FROM 
    {{ source('raw', 'KENP') }}

WHERE Date != 'Date'