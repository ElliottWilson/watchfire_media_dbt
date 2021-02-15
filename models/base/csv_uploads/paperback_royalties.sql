SELECT
  Royalty_Date AS royalty_date,
  Order_Date AS order_date,
  Title AS title,
  Author_Name AS author_name,
  ISBN AS ISBN_code,
  Marketplace AS marketplace,
  Royalty_Type AS royalty_type,
  Transaction_Type AS transaction_type,
  Units_Sold AS units_sold,
  Units_Refunded AS units_refunded,
  Net_Units_Sold AS net_units_sold,
  Avg__List_Price_without_tax AS avg_list_price_without_tax,
  Avg__Offer_Price_without_tax AS avg_offer_price_without_tax,
  Avg__Manufacturing_Cost AS avg_manufacturing_cost,
  Royalty AS royalty,
  Currency AS currency
FROM 
    {{ source('raw', 'paperback_royalties') }}
