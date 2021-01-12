SELECT
    *
FROM 
{{ source('raw_openbridge', 'amzadvertising_sp_targets_v4') }}