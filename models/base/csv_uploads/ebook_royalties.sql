WITH ebook_royalties_id AS 

(SELECT *, md5(name||time_of_entry||royalty_date||title||author_name||ASIN_code||marketplace||royalty_type||
transaction_type||units_sold||units_refunded||net_units_sold||avg_list_price_without_tax||
avg_offer_price_without_tax||avg_file_size_MB||avg_delivery_cost||royalty||currency) AS id
FROM {{source('raw', 'ebook_royalties')}}),

entry_time AS 
(SELECT 
*,
MAX(time_of_entry) OVER (PARTITION BY id) AS max_time_of_entry,
FROM ebook_royalties_id)

SELECT
  * EXCEPT (avg_delivery_cost), 
  CASE avg_delivery_cost
  WHEN 'N/A' THEN '0'
  ELSE avg_delivery_cost
  END AS avg_delivery_cost
FROM entry_time
WHERE time_of_entry = max_time_of_entry
