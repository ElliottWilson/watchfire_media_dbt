WITH kenp_table AS 
(SELECT
 *,  
EXTRACT(MONTH FROM date) AS month,
EXTRACT(YEAR FROM date) AS year,
FROM 
    {{ source('raw', 'kenp') }}) 

SELECT *, 
(kenp_table.month ||'-'|| kenp_table.year ) AS month_year

FROM kenp_table