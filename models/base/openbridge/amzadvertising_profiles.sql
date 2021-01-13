SELECT
    *
FROM 
{{ source('raw_openbridge', 'amzadvertising_profiles_master') }}