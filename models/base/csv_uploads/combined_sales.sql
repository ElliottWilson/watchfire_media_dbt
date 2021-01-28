SELECT
  Royalty_Date AS royalty_date,
  EXTRACT(MONTH FROM Royalty_Date) AS month,
  Title AS title,
  Author_Name AS author_name,
  ASIN_ISBN AS ASIN_ISBN,
  Marketplace AS marketplace,
  Royalty_Type AS royalty_type,
  Transaction_Type AS transaction_type,
  IFNULL(CAST(Units_Sold AS numeric),0) AS units_sold,
  IFNULL(CAST(Units_Refunded AS numeric),0) AS units_refunded,
  IFNULL(CAST(Net_Units_Sold AS numeric),0) AS net_units_sold,
  IFNULL(CAST(Avg__List_Price_without_tax AS numeric),0) AS avg_list_price_without_tax,
  IFNULL(CAST(Avg__Offer_Price_without_tax AS numeric),0) AS avg_offer_price_without_tax,
  CASE WHEN Avg__Delivery_Manufacturing_cost = 'N/A' THEN 0
       ELSE IFNULL(CAST(Avg__Delivery_Manufacturing_cost AS numeric),0)
  END AS avg_delivery_Manufacturing_cost,
  IFNULL(CAST(Royalty AS numeric),0) AS royalty,
  Currency AS currency
FROM 
    {{ source('raw', 'combined_sales_csv') }}
WHERE Royalty_Date IS NOT NULL