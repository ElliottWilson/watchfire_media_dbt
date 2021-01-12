SELECT
Marketplace AS marketplace,
Region AS region, 
BookBub_Region AS bookBub_region
{{ source('raw', 'region') }}