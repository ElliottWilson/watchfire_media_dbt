 -- depends_on: {{ ref('data_feeds') }}
 -- depends_on: {{ ref('amzadvertising_profiles') }}
{% set accounts = get_column_values(table=ref('data_feeds'), column='account', max_records=50, filter_column='platform', filter_value='Amazon Ads', filter_column_2='Status', filter_value_2='Live') %}

{% if accounts != [] %}
      {% for account in accounts %}
                {% set author = get_column_values(table=ref('data_feeds'), column='author', max_records=50, filter_column='platform', filter_value='Amazon Ads', filter_column_2='account', filter_value_2=account) %}
        SELECT
        author_unested AS author,
        'Amazon Ads' AS platform,
        amzadvertising_sp_productads_master.* EXCEPT (cost),
        IFNULL(CAST(cost AS numeric),0) AS cost
        FROM  {{ source('raw_openbridge', 'amzadvertising_sp_productads_master') }} AS amzadvertising_sp_productads_master, UNNEST({{author}}) AS author_unested
        INNER JOIN {{ ref('amzadvertising_profiles') }} AS amzadvertising_profiles
        ON CAST(amzadvertising_sp_productads_master.profile_id AS STRING) = CAST(amzadvertising_profiles.profile_id AS STRING)
        AND amzadvertising_sp_productads_master.ob_date = amzadvertising_profiles.ob_date
        WHERE CAST(amzadvertising_profiles.account_info_id AS STRING) = '{{account}}'
	    	{% if not loop.last %} UNION ALL {% endif %}
            {% endfor %}
 {% endif %}