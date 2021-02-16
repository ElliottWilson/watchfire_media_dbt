WITH entry_time AS 

(SELECT 
*,
MAX(time_of_entry) OVER (PARTITION BY order_date, marketplace, ASIN_code) AS max_time_of_entry,
FROM {{ source('raw', 'amazon_orders') }})


SELECT *
FROM entry_time 

WHERE max_time_of_entry = time_of_entry

