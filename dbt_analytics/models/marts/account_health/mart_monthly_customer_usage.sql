{{ config(materialized="table") }}

/*

Used for TrafficPeak micro-site

*/

with ie_tables as (
    select
        id as ie_table_id,
        ie_project__c as ie_project_id
    from raw_salesforce.ie_table__c
    where is_deleted is False
), ie_projects as (
    select
        id as ie_project_id,
        ie_cluster__c as ie_cluster_id,
        deployment__c as deployment_id
    from raw_salesforce.ie_project__c
    where is_deleted is False
), ie_clusters as (
    select
        id as ie_cluster_id,
        deployment__c as deployment_id,
        hdx_shared_cluster__c as is_shared
    from raw_salesforce.ie_cluster__c
    where is_deleted is False
), deployments as (
    select
        id as deployment_id,
        contract__c as contract_id,
        opportunity__c as opportunity_id,
        account__c as account_id
    from raw_salesforce.deployment__c
    where is_deleted is False
), ie_table_usage as (
    select
        ie_table__c as ie_table_id,
        invoice_month__c as invoice_month,
        total_bytes__c as total_bytes,
        total_rows__c as total_rows
    from {{ source('raw_salesforce', 'ie_table_usage__c') }}
    where is_deleted is False
), ie_project_queries as (
    select
        ie_project__c as ie_project_id,
        invoice_month__c as invoice_month,
        max_qpm__c as max_qpm,
        avg_daily_max_qpm__c as avg_daily_max_qpm
    from {{ source('raw_salesforce', 'ie_project_queries__c') }}
    where is_deleted is False
), ie_cluster_queries as (
    select
        ie_cluster__c as ie_cluster_id,
        invoice_month__c as invoice_month,
        max_qpm__c as max_qpm,
        avg_daily_max_qpm__c as avg_daily_max_qpm
    from {{ source('raw_salesforce', 'ie_cluster_queries__c') }}
    where is_deleted is False
), ie_table_usage_by_project_by_month as (
    select
        p.ie_project_id,
        u.invoice_month,
        sum(u.total_bytes) as total_bytes,
        sum(u.total_rows) as total_rows
    from ie_table_usage u
    left join ie_tables t on u.ie_table_id = t.ie_table_id
    left join ie_projects p on t.ie_project_id = p.ie_project_id
    group by 1, 2
), ie_table_usage_with_queries_by_project_by_month as (
    select
        u.ie_project_id,
        u.invoice_month as reporting_month,
        u.total_bytes,
        u.total_rows,
        q.max_qpm,
        q.avg_daily_max_qpm
    from ie_table_usage_by_project_by_month u
    left join ie_project_queries q
        on u.ie_project_id = q.ie_project_id
        and u.invoice_month = q.invoice_month
), ie_usage_and_queries_with_deployment as (
    select
        u.ie_project_id,
        u.reporting_month,
        u.total_bytes,
        u.total_rows,
        case
            when c.is_shared then u.max_qpm
            else ie_c_q.max_qpm
        end as max_qpm,
        case
            when c.is_shared then u.avg_daily_max_qpm
            else ie_c_q.avg_daily_max_qpm
        end as avg_daily_max_qpm,
        case
            when c.is_shared then p.deployment_id
            else c.deployment_id
        end as deployment_id
    from ie_table_usage_with_queries_by_project_by_month u
    left join ie_projects p on u.ie_project_id = p.ie_project_id
    left join ie_clusters c on p.ie_cluster_id = c.ie_cluster_id
    left join ie_cluster_queries ie_c_q
        on c.ie_cluster_id = ie_c_q.ie_cluster_id
        and u.reporting_month = ie_c_q.invoice_month
), ie_usage_and_queries_grouped_by_deployment as (
    select
        deployment_id,
        reporting_month,
        sum(total_bytes) as total_bytes,
        sum(total_rows) as total_rows,
        max(max_qpm) as max_qpm,
        avg(avg_daily_max_qpm) as avg_daily_max_qpm
    from ie_usage_and_queries_with_deployment
    group by 1, 2
), accounts as (
    select
        id as account_id,
        name as account_name,
        akamai_account_id__c as akamai_account_id,
        primary_industry__c as primary_industry,
        primary_sub_industry__c as primary_sub_industry,
        sub_industry__c as sub_industry
    from raw_salesforce.account
    where is_deleted is False
), contracts as (
    select
        a.account_name as account_name,
        c.contract_number,
        c.start_date,
        case
            when c.contract_overrun__c is False then c.end_date
            else CURRENT_DATE
        end as end_date,
        c.id as contract_id,
        c.account_id as account_id,
        c.opportunity__c as opportunity_id,
        c.created_date,
        d.deployment_id,
        c.tb_per_month_standard__c as tb_per_month_standard,
        c.commit_amount__c as commit_amount,
        c.commit_type__c as commit_type,
        c.akamai_contract_id__c as akamai_contract_id,
        a.akamai_account_id as akamai_account_id,
        mrr__c as mrr,
        region__c as sales_region,
        hydrolix_product__c as hydrolix_product,
        pricing_calculator_version__c as pricing_calculator_version,
        c.contract_overrun__c as contract_overrun,
        a.primary_industry,
        a.primary_sub_industry,
        a.sub_industry,
        c.country__c as country
    from raw_salesforce.contract c
    left join deployments d on c.id = d.contract_id
    left join accounts a on c.account_id = a.account_id
    where c.is_deleted is False
    and (
        c.contract_active__c is True or
        c.contract_overrun__c is True
    )
), opportunities as (
    select
        a.account_name as account_name,
        o.name as opportunity_name,
        o.stage_name as stage_name,
        o.poc_start_date__c as start_date,
        o.poc_end_date__c as end_date,
        o.poc_requested_date__c as poc_requested_date,
        o.last_poc_credit_month__c as poc_retirement_date,
        o.poc_status__c as poc_status,
        o.id as opportunity_id,
        o.account_id as account_id,
        d.deployment_id,
        o.created_date,
        (CURRENT_DATE - o.created_date) as poc_age_days,
        a.akamai_account_id as akamai_account_id,
        mrr__c as mrr,
        region__c as sales_region,
        hydrolix_service__c as hydrolix_product,
        pricing_calculator_version__c as pricing_calculator_version,
        a.primary_industry,
        a.primary_sub_industry,
        a.sub_industry,
        o.country__c as country
    from raw_salesforce.opportunity o
    left join deployments d on o.id = d.opportunity_id
    left join accounts a on o.account_id = a.account_id
    where o.is_deleted is False
    and o.active_poc__c is True
    and o.channel__c != 'INTERNAL'
), deal_union as (
    select
        account_name,
        contract_number,
        'N/A' as opportunity_name,
        false as is_poc,
        opportunity_id,
        contract_id,
        account_id,
        deployment_id,
        mrr,
        sales_region,
        hydrolix_product,
        pricing_calculator_version,
        created_date,
        start_date,
        end_date,
        null as stage_name,
        null as poc_requested_date,
        null as poc_retirement_date,
        null as poc_status,
        null as poc_age_days,
        tb_per_month_standard,
        commit_amount,
        commit_type,
        date_trunc('month', start_date)::date as start_month,
        date_trunc('month', end_date)::date as end_month,
        akamai_contract_id,
        akamai_account_id,
        contract_overrun,
        primary_industry,
        primary_sub_industry,
        sub_industry,
        country
    from contracts
        union all
    select
        account_name,
        'N/A' as contract_number,
        opportunity_name,
        true as is_poc,
        opportunity_id,
        null as contract_id,
        account_id,
        deployment_id,
        mrr,
        sales_region,
        hydrolix_product,
        pricing_calculator_version,
        created_date,
        start_date,
        end_date,
        stage_name,
        poc_requested_date,
        poc_retirement_date,
        poc_status,
        poc_age_days,
        null as tb_per_month_standard,
        null as commit_amount,
        null as commit_type,
        date_trunc('month', start_date)::date as start_month,
        date_trunc('month', end_date)::date as end_month,
        null as akamai_contract_id,
        akamai_account_id,
        False as contract_overrun,
        primary_industry,
        primary_sub_industry,
        sub_industry,
        country
    from opportunities
), months AS (
    SELECT * FROM {{ ref('dim_month') }}
    WHERE month_date BETWEEN '2025-01-01' AND CURRENT_DATE
), deals_with_month as (
    select
        d.*,
        m.month_date as reporting_month
    from deal_union d
    inner join months m
        on d.start_month <= m.month_date
        and d.end_month >= m.month_date
), deals_with_usage as (
    select
        d.*,
        coalesce(u.total_bytes, 0) as total_bytes,
        coalesce(u.total_rows, 0) as total_rows,
        coalesce(u.max_qpm, 0) as max_qpm,
        coalesce(u.avg_daily_max_qpm, 0) as avg_daily_max_qpm,
        coalesce((u.total_bytes / (1024^4)), 0) as total_tib,
        coalesce((u.total_bytes / (1000^4)), 0) as total_tb,
        coalesce((u.total_rows / 1000000000), 0) as total_billion_rows
    from deals_with_month d
    left join ie_usage_and_queries_grouped_by_deployment u
        on d.deployment_id = u.deployment_id
        and d.reporting_month = u.reporting_month
), deals_with_commit as (
    select
        d.*,
        case
            when commit_type = 'Billion records per month' then total_billion_rows / nullif(commit_amount, 0)
            else total_tb / nullif(tb_per_month_standard, 0)
        end as pct_of_commit, -- TB Standard + Billion records per month % of commit
        case
            when commit_type = 'Billion records per month' then total_billion_rows / nullif(commit_amount, 0)
            else total_tb / nullif(tb_per_month_standard, 0)
        end as pct_of_commit_pro_rated
    from deals_with_usage d
), deals_with_flags as (
    select
        d.*,
        case
            when is_poc is False then ''
            when poc_retirement_date is null then 'No Credits'
            when poc_retirement_date < current_date then 'Expired'
            when poc_retirement_date >= date_trunc('month', current_date) and poc_retirement_date < date_trunc('month', current_date + interval '1 month' - interval '1 day') then 'Expiring Before EOM'
            else ''
        end as poc_flag,
        case
            when is_poc is True then ''
            when contract_overrun is True then 'Expired'
            when end_date >= date_trunc('quarter', current_date) and end_date < date_trunc('quarter', current_date + interval '90 days' - interval '1 day') then 'Expiring This Quarter'
            else ''
        end as contract_flag,
        case
            when is_poc then
                case
                    when total_bytes = 0 then 'No Ingest'
                    when total_bytes < (1000^3) then 'Low Ingest'
                    else ''
                end
            else
                case
                    when pct_of_commit < 0.01 then 'No Ingest'
                    when pct_of_commit < 0.25 then 'Low Ingest'
                    else ''
                end
        end as ingest_flag,
        case
            when max_qpm = 0 then 'No Queries'
            when max_qpm < 2 then 'Low Queries'
            else ''
        end as query_flag
    from deals_with_commit d
), deals_with_status as (
    select
        d.*,
        case
            when d.pct_of_commit > 1.0 then 'Upgrade'
            when poc_flag != '' or contract_flag != '' or ingest_flag != '' then 'Warning'
            else 'Happy'
        end as status
    from deals_with_flags d
)
select * from deals_with_status
