{{config(materialized="table")}}

/*
===============================================================================
Model:
    fct_usage__project_daily

Grain:
    One row per (cluster_hostname, project_name, reporting_date)

Description:
    Daily infrastructure-level usage aggregated at the cluster + project level.
    Combines storage/row consumption data with peak query performance metrics.
    This model represents raw technical usage prior to mapping to Salesforce
    deployments or revenue contracts.

Metrics:
    - total_bytes  (sum of bytes processed per day)
    - total_rows   (sum of rows processed per day)
    - max_qpm      (maximum queries per minute observed per day)
===============================================================================
*/

with usage_data as (
    select
        date as reporting_date,
        cluster_hostname,
        project_name,
        concat(cluster_hostname, '-', project_name) as cluster_project_name,
        sum(total_bytes) as total_bytes,
        sum(total_rows) as total_rows
    from {{ source('argus', 'daily_usage_with_table') }}
    where cluster_hostname is not null
    and project_name is not null
    and table_name is not null
    and date >= '2025-01-01'
    and project_name != 'hydro'
    group by 1, 2, 3, 4
), query_data as (
    select
        date as reporting_date,
        cluster_hostname,
        project_name,
        concat(cluster_hostname, '-', project_name) as cluster_project_name,
        max(max_qpm) as max_qpm,
        sum(customer_queries) as total_queries
    from {{ source('argus', 'daily_query_with_table') }}
    where date >= '2025-01-01'
    group by 1, 2, 3, 4
), usage_with_query as (
    select
        u.cluster_hostname,
        u.project_name,
        u.cluster_project_name,
        u.reporting_date,
        u.total_rows,
        u.total_bytes,
        q.max_qpm,
        q.total_queries
    from usage_data u
    left join query_data q
        on u.cluster_project_name = q.cluster_project_name
        and u.reporting_date = q.reporting_date
)
select * from usage_with_query
