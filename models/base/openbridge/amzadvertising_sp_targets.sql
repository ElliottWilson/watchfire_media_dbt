 -- depends_on: {{ ref('data_feeds') }}
 -- depends_on: {{ ref('amzadvertising_profiles') }}
{% set accounts = get_column_values(table=ref('data_feeds'), column='account', max_records=50, filter_column='platform', filter_value='Amazon Ads', filter_column_2='Status', filter_value_2='Live') %}

{% if accounts != [] %}
      {% for account in accounts %}
        SELECT
        amzadvertising_sp_targets_master.* EXCEPT (cost),
        IFNULL(CAST(cost AS numeric),0) AS cost
        FROM  {{ source('raw_openbridge', 'amzadvertising_sp_targets_master') }} AS amzadvertising_sp_targets_master
        INNER JOIN {{ ref('amzadvertising_profiles') }} AS amzadvertising_profiles
        ON CAST(amzadvertising_sp_targets_master.profile_id AS STRING) = CAST(amzadvertising_profiles.profile_id AS STRING)
        AND amzadvertising_sp_targets_master.ob_date = amzadvertising_profiles.ob_date
        WHERE CAST(amzadvertising_profiles.account_info_id AS STRING) = '{{account}}'
	    	{% if not loop.last %} UNION ALL {% endif %}
            {% endfor %}
 {% endif %}