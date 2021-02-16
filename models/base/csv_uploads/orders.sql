SELECT 
 *
FROM
    {{ source('raw', 'amazon_orders') }}
