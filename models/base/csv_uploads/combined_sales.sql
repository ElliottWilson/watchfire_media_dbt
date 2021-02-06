SELECT
  CAST(Royalty_Date AS DATE) AS royalty_date,
  EXTRACT(MONTH FROM CAST(Royalty_Date AS DATE)) AS month,
  Title AS title,
  Author_Name AS author_name,
  ASIN_ISBN AS ASIN_ISBN,
  Marketplace AS marketplace,
  CAST(Royalty_Type AS FLOAT64) AS royalty_type,
  Transaction_Type AS transaction_type,
  IFNULL(CAST(CAST(Units_Sold AS INT64) AS numeric),0) AS units_sold,
  IFNULL(CAST(CAST(Units_Refunded AS INT64) AS numeric),0) AS units_refunded,
  IFNULL(CAST(CAST(Net_Units_Sold AS INT64) AS numeric),0) AS net_units_sold,
  IFNULL(CAST(CAST(Avg__List_Price_without_tax AS FLOAT64) AS numeric),0) AS avg_list_price_without_tax,
  IFNULL(CAST(CAST(Avg__Offer_Price_without_tax AS FLOAT64) AS numeric),0) AS avg_offer_price_without_tax,
  CASE WHEN Avg__Delivery_Manufacturing_cost = 'N/A' THEN 0
       ELSE IFNULL(CAST(Avg__Delivery_Manufacturing_cost AS numeric),0)
  END AS avg_delivery_Manufacturing_cost,
  IFNULL(CAST(CAST(Royalty AS FLOAT64) AS numeric),0) AS royalty,
  Currency AS currency
FROM 
    {{ source('raw', 'combined_sales') }}
WHERE Royalty_Date IS NOT NULL
AND Royalty_Date != 'Royalty_Date'