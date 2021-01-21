WITH
  region AS (
  SELECT
    Marketplace AS marketplace,
    Region.Region AS region,
    BookBub_Region AS bookBub_region
  FROM
    {{ source('raw',
      'region') }})
SELECT
  region.marketplace,
  region.region,
  bookBub_region,
  region_to_currency_mapping.currency_code
FROM
  region
LEFT JOIN
  {{ ref('region_to_currency_mapping') }} AS region_to_currency_mapping
ON
  region.region = region_to_currency_mapping.clean_country_code
WHERE
  marketplace != 'Marketplace'






