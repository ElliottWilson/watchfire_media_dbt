SELECT
    *
FROM 
{{ source('raw_openbridge', 'fbm_account_master') }}