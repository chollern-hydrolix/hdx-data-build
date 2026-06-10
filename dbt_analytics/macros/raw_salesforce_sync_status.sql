{% macro raw_salesforce_sync_status() %}

    {#
        Generates a UNION ALL of max(taskengine_last_sync_ts) queries across every
        table in the raw_salesforce schema that has a taskengine_last_sync_ts column.

        Uses run_query() which executes against the database at model runtime.
        Guarded by {% if execute %} so it is a no-op during dbt parse/compile.

        Returns:
            raw_table_name  text        -- lowercase table name in raw_salesforce
            last_sync_ts    timestamptz -- most recent ETL sync timestamp
    #}

    {% set tables_query %}
        select table_name
        from information_schema.columns
        where table_schema = 'raw_salesforce'
          and column_name = 'taskengine_last_sync_ts'
        order by table_name
    {% endset %}

    {% set results = run_query(tables_query) %}

    {% if execute %}
        {% set table_names = results.columns[0].values() %}
    {% else %}
        {% set table_names = [] %}
    {% endif %}

    {% set union_parts = [] %}

    {% for table_name in table_names %}
        {% do union_parts.append("select '" ~ table_name ~ "'::text as raw_table_name, max(taskengine_last_sync_ts) as last_sync_ts from raw_salesforce." ~ table_name) %}
    {% endfor %}

    {% if union_parts | length > 0 %}
        {{ union_parts | join(' union all ') }}
    {% else %}
        select null::text as raw_table_name, null::timestamptz as last_sync_ts where false
    {% endif %}

{% endmacro %}