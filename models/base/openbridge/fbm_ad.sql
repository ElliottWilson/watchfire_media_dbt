SELECT
    *
FROM 
{{ source('raw_openbridge', 'fbm_ad_v8') }}