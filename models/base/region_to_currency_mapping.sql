SELECT
  Country AS country,
  CountryCode AS country_code,
  Currency AS currency,
  Code AS currency_code,
CASE
    WHEN Code = 'EUR' THEN 'EU'
    WHEN Code = 'USD' THEN 'US'
    WHEN Code = 'XOF' THEN 'WA'
    WHEN Code = 'AUD' THEN 'AU'
    WHEN Code = 'XCD' THEN 'EC'
    WHEN Code = 'GBP' THEN 'UK'
    WHEN Code = 'XAF' THEN 'CA'
    WHEN Code = 'NZD' THEN 'NZ'
    WHEN Code = 'XPF' THEN 'FP'
    WHEN Code = 'DKK' THEN 'DK'
    WHEN Code = 'NOK' THEN 'NO'
    WHEN Code = 'ANG' THEN 'NL'
    WHEN Code = 'LAK' THEN 'LA'
    WHEN Code = 'MAD' THEN 'MA'
    WHEN Code = 'MOP' THEN 'MO'
    WHEN Code = 'CHF' THEN 'CH'
    WHEN Code = 'INR' THEN 'IN'
    WHEN Code = 'ETB' THEN 'ET'
    WHEN Code = 'CDF' THEN 'CD'
    WHEN Code = 'JOD' THEN 'JO'
    WHEN Code = 'IDR' THEN 'ID'
 
    ELSE CountryCode
END AS clean_country_code 

FROM
    {{ source('raw', 'region_to_currency_mapping') }}