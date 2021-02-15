SELECT
CAST(Month AS INT64) AS month,
Currency AS currency_code,
Month_Currency AS month_currency_code,
IFNULL(CAST(CAST(USD_to_Currency AS FLOAT64) AS numeric),0) AS conversion_to_USD
FROM 
{{ source('raw', 'currencies') }}

WHERE Month != 'Month'