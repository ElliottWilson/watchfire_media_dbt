SELECT
  Royalty_Date AS royalty_date,
  Title AS title,
  Author_Name AS author_name,
  ASIN AS ASIN_code,
  Marketplace AS marketplace,
  Royalty_Type AS royalty_type,
  Transaction_Type AS transaction_type,
  Units_Sold AS units_sold,
  Units_Refunded AS units_refunded,
  Net_Units_Sold AS net_units_sold,
  Avg__List_Price_without_tax AS avg_list_price_without_tax,
  Avg__Offer_Price_without_tax AS avg_offer_price_without_tax,
  Avg__File_Size__MB_ AS avg_file_size_MB,
  Avg__Delivery_Cost AS avg_delivery_cost,
  Royalty AS royalty,
  Currency AS currency
FROM 
{{ source('raw', 'ebook_royalty_csv') }}