WITH entry_time AS 
(SELECT 
*,
MAX(time_of_entry) OVER (PARTITION BY month_year, currency) AS max_time_of_entry,
FROM {{ source('raw', 'currencies') }})


SELECT 
month_year, 
currency, 
currency_usd, 
usd_currency, 
month_year_currency
FROM 
entry_time

WHERE time_of_entry = max_time_of_entry