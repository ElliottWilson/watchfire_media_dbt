SELECT
  * EXCEPT (avg_delivery_cost), 
  CASE avg_delivery_cost
  WHEN 'N/A' THEN '0'
  ELSE avg_delivery_cost
  END AS avg_delivery_cost
FROM 
{{ source('raw', 'ebook_royalties') }}