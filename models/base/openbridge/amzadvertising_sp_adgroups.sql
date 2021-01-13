SELECT
    *
FROM 
{{ source('raw_openbridge', 'amzadvertising_sp_adgroups_master') }}