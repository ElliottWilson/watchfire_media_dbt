WITH entry_time AS 

(SELECT 
*,
MAX(time_of_entry) OVER (PARTITION BY month_year_region) AS max_time_of_entry,
FROM {{ source('raw', 'amzn_ebook_rates') }})

SELECT
*
FROM entry_time

WHERE time_of_entry = max_time_of_entry
