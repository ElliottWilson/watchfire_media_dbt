SELECT
Marketplace AS marketplace,
Region.Region AS region, 
BookBub_Region AS bookBub_region
FROM 
{{ source('raw', 'region') }}
WHERE Marketplace != 'Marketplace'