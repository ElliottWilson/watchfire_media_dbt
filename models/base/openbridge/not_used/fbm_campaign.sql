SELECT
    *
FROM 
{{ source('raw_openbridge', 'fbm_campaign_master') }}