SELECT
    *
FROM 
{{ source('raw_openbridge', 'amzadvertising_sp_campaigns_master') }}