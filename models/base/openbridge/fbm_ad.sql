SELECT 
* EXCEPT (campaign_id,ob_date,spend), 
CAST(campaign_id AS INT64) AS campaign_id,
CAST(ob_date AS DATE) AS ob_date,
IFNULL(CAST(spend AS numeric),0) AS spend
FROM {{ source('raw_openbridge', 'fbm_ad_master') }}