WITH book_union AS (
SELECT
  DISTINCT title,
  author_name,
  CAST(ASIN_code AS STRING) AS ASIN_code,
  marketplace
FROM
  {{ ref('orders') }}
UNION DISTINCT
SELECT
  DISTINCT title,
  author_name,
  CAST(ASIN_code AS STRING) AS ASIN_code,
  marketplace
FROM
  {{ ref('KENP') }}
UNION DISTINCT
SELECT
  DISTINCT title,
  author_name,
  CAST(ISBN_code AS STRING) AS ASIN_code,
  marketplace
FROM
  {{ ref('paperback_royalties') }}
UNION DISTINCT
SELECT
  DISTINCT title,
  author_name,
  CAST(ASIN_code AS STRING) AS ASIN_code,
  marketplace
FROM
  {{ ref('ebook_royalties') }}
)

SELECT
  book_union.title,
  book_union.author_name,
  book_union.ASIN_code AS ASIN_ISBN,
  book_union.marketplace,
  region.region,
  region.bookBub_region,
  (book_union.ASIN_code ||'.'|| region.region) AS ASIN_region
FROM
  book_union
LEFT JOIN
  {{ ref('region') }} AS region
ON
  book_union.marketplace = region.marketplace

