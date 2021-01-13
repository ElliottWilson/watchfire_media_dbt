SELECT
    *
FROM 
{{ source('raw_openbridge', 'fbm_adset_master') }}