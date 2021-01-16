SELECT
  Country AS country,
  CountryCode AS country_code,
  Currency AS currency,
  Code AS currency_code
FROM
    {{ source('raw', 'region_to_currency_mapping') }}