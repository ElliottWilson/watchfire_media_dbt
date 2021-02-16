WITH entry_time AS 

(SELECT 
*,
MAX(time_of_entry) OVER (PARTITION BY royalty_date, marketplace, ISBN_code, transaction_type) AS max_time_of_entry,
FROM {{ source('raw', 'paperback_royalties') }})

SELECT
 *
FROM entry_time

WHERE time_of_entry = max_time_of_entry
   
