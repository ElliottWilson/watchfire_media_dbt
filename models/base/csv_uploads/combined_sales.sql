WITH combined_sales_id AS 

(SELECT *, md5(name||time_of_entry||royalty_date||title||author_name||ASIN_ISBN||marketplace||
royalty_type||transaction_type||units_sold||units_refunded||net_units_sold||avg_list_price_without_tax||
avg_offer_price_without_tax||avg_delivery_manufacturing_cost||royalty||currency) AS id 
FROM {{ source('raw', 'combined_sales') }}),

entry_time AS 
(SELECT 
*,
MAX(time_of_entry) OVER (PARTITION BY id) AS max_time_of_entry,
CAST(EXTRACT(MONTH FROM royalty_date) as string) AS month,
CAST(EXTRACT(YEAR FROM royalty_date)  as string) AS year
FROM combined_sales_id), 

combined_sales AS 
 (SELECT
* EXCEPT (units_sold, units_refunded, net_units_sold, avg_list_price_without_tax, avg_offer_price_without_tax, avg_delivery_manufacturing_cost, royalty),
  CONCAT(month,"-",year) AS month_year,
  IFNULL(CAST(units_sold AS numeric),0) AS units_sold,
  IFNULL(CAST(units_refunded AS numeric),0) AS units_refunded,
  IFNULL(CAST(net_units_sold AS numeric),0) AS net_units_sold,
  IFNULL(CAST(avg_list_price_without_tax AS numeric),0) AS avg_list_price_without_tax,
  IFNULL(CAST(avg_offer_price_without_tax AS numeric),0) AS avg_offer_price_without_tax,
  CASE WHEN avg_delivery_manufacturing_cost = 'N/A' THEN 0
       ELSE IFNULL(CAST(avg_delivery_manufacturing_cost AS numeric),0)
  END AS avg_delivery_manufacturing_cost,
  IFNULL(CAST(royalty AS numeric),0) AS royalty
FROM 
    entry_time
WHERE time_of_entry = max_time_of_entry
AND Royalty_Date IS NOT NULL)

SELECT *

FROM combined_sales