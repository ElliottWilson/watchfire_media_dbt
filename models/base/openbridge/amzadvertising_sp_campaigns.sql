SELECT
    *
FROM 
{{ source('raw_openbridge', 'amzadvertising_sp_campaigns_placement_v1') }}