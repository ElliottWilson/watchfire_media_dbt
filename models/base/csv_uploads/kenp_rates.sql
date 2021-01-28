SELECT
Month AS month,
Region AS region, 
Month_Region AS month_region,
Currency AS currency, 
Rate_Native_Currency_pg AS rate_native_currency_pg, 
Rate_per_page AS rate_USD_pg
FROM 
{{ source('raw', 'amzn_ebook_rates') }}
WHERE month != 'Month'