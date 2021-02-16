WITH entry_time AS 
(SELECT 
*,
MAX(time_of_entry) OVER (PARTITION BY date, marketplace, ASIN_code) AS max_time_of_entry,
FROM {{ source('raw', 'kenp') }}),

 kenp_table AS 
(SELECT
 *,  
EXTRACT(MONTH FROM date) AS month,
EXTRACT(YEAR FROM date) AS year,
FROM 
   entry_time) 

SELECT *, 
(kenp_table.month ||'-'|| kenp_table.year ) AS month_year

FROM kenp_table

WHERE time_of_entry = max_time_of_entry