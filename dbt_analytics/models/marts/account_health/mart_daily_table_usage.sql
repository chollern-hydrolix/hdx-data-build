{{ config(materialized="table") }}

with days as (
    SELECT * FROM {{ ref('dim_day') }}
    WHERE day_date BETWEEN '2025-01-01' AND CURRENT_DATE
), contract_data as (
    select
        a.account_name,
        c.contract_number,
        a.akamai_account_id,
        c.akamai_contract_id,
        c.contract_start_date,
        c.contract_end_date,
        concat(d.clean_cluster_hostname, '-', d.project_name) as cluster_project,
        a.account_id,
        c.contract_id,
        d.deployment_id
    from {{ref('fct_contract')}} c
    left join {{ref('dim_account')}} a on c.account_id = a.account_id
    left join {{ref('fct_deployment')}} d on c.contract_id = d.contract_id
    -- where d.deployment_id is not null
    -- and c.status = 'Activated'
), contracts_with_days as (
    select
        c.*,
        d.day_date as reporting_date,
        d.month_date as reporting_month,
        extract(day from (date_trunc('month', d.month_date) + interval '1 month - 1 day')) as total_days_in_month
    from contract_data c
    inner join days d
        on c.contract_start_date <= d.day_date
        and c.contract_end_date >= d.day_date
), usage_data as (
    select
        date as reporting_date,
        concat(cluster_hostname, '-', project_name) as cluster_project,
        cluster_hostname,
        project_name,
        table_name,
        sum(total_bytes) as total_bytes,
        sum(total_rows) as total_rows
    from argus.daily_usage_with_table
    where cluster_hostname is not null
    and project_name is not null
    and table_name is not null
    group by 1, 2, 3, 4, 5
), query_data as (
    select
        date as reporting_date,
        concat(cluster_hostname, '-', project_name) as cluster_project,
        max(max_qpm) as max_qpm
    from argus.daily_query_with_table
    group by 1, 2
), contracts_with_usage as (
    select
        c.*,
        u.cluster_hostname,
        u.project_name,
        u.table_name,
        u.total_bytes,
        u.total_rows,
        q.max_qpm
    from contracts_with_days c
    left join usage_data u
        on c.reporting_date = u.reporting_date
        and c.cluster_project = u.cluster_project
    left join query_data q
        on c.reporting_date = q.reporting_date
        and c.cluster_project = q.cluster_project
)
select * from contracts_with_usage
