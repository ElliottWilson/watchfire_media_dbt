SELECT
    *
FROM 
{{ source('raw_openbridge', 'amzadvertising_sp_keywords_v4') }}