
SELECT
Month AS month,
Currency AS currency_code, 
Month_Currency AS month_currency_code,
Currency_to_USD AS conversion_to_USD
FROM 
{{ source('raw', 'currencies') }}