SELECT
Month AS month,
Region AS region, 
Month_Region AS month_region,
Currency AS currency, 
IFNULL(CAST(Rate_Native_Currency_pg AS numeric),0) AS rate_native_currency_pg, 
IFNULL(CAST(REPLACE(Rate_per_page,'$', '') AS numeric),0) AS rate_USD_pg
FROM 
{{ source('raw', 'amzn_ebook_rates') }}
WHERE month != 'Month'