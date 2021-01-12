SELECT
  Title AS title,
  Region AS region,
  Author AS author,
  ASIN_ISBN AS ASIN_ISBN,
  Series AS series,
  Book_number AS book_number,
  ASIN_Region AS ASIN_region
FROM
    {{ source('raw', 'portfolio') }}