 -- depends_on: {{ ref('data_feeds') }}
{% set accounts = get_column_values(table=ref('data_feeds'), column='account', max_records=50, filter_column='platform', filter_value='Facebook Ads', filter_column_2='Status', filter_value_2='Live') %}

{% if accounts != [] %}
      {% for account in accounts %}
        SELECT
        * EXCEPT (campaign_id,ob_date,spend), 
        CAST(campaign_id AS INT64) AS campaign_id,
        CAST(ob_date AS DATE) AS ob_date,
        IFNULL(CAST(spend AS numeric),0) AS spend,
        FROM {{ source('raw_openbridge', 'fbm_ad_master') }}
        WHERE account_name = '{{account}}'
	    	{% if not loop.last %} UNION ALL {% endif %}
            {% endfor %}
 {% endif %}