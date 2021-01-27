SELECT 
* EXCEPT (campaign_id,ob_date), 
CAST(campaign_id AS INT64) AS campaign_id,
CAST(ob_date AS DATE) AS ob_date
FROM {{ source('raw_openbridge', 'fbm_ad_master') }}