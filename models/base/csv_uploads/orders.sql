SELECT 
  CAST(Order_Date AS Date) AS order_date,
  Title AS title,
  Author_Name AS author_name,
  ASIN AS ASIN_code,
  Marketplace AS marketplace,
  Paid_Units  AS paid_units,
  Free_Units AS free_units
FROM
    {{ source('raw', 'amazon_orders') }}

WHERE Order_Date != 'Order_Date'