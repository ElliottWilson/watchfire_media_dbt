SELECT
  Royalty_Date AS royalty_date ,
  Title AS title,
  Author_Name AS author_name,
  ASIN_ISBN AS ASIN_ISBN,
  Marketplace AS marketplace,
  Royalty_Type AS royalty_type,
  Transaction_Type AS transaction_type,
  Units_Sold AS units_sold,
  Units_Refunded AS units_refunded,
  Net_Units_Sold AS net_units_sold,
  Avg__List_Price_without_tax AS avg_list_price_without_tax,
  Avg__Offer_Price_without_tax AS avg_offer_price_without_tax,
  Avg__Delivery_Manufacturing_cost AS avg_delivery_Manufacturing_cost,
  Royalty AS royalty,
  Currency AS currency
FROM 
    {{ source('raw', 'KDP_sales_dashboard_csv') }}
WHERE Royalty_Date IS NOT NULL