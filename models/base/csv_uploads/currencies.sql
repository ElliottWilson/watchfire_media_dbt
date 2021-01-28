SELECT
Month AS month,
Currency AS currency_code,
Month_Currency AS month_currency_code,
IFNULL(CAST(Currency_to_USD AS numeric),0) AS conversion_to_USD
FROM 
{{ source('raw', 'currencies') }}