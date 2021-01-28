SELECT
* EXCEPT (cost),
IFNULL(CAST(cost AS numeric),0) AS cost
FROM 
{{ source('raw_openbridge', 'amzadvertising_sp_targets_master') }}