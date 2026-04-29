{{
    config(
        materialized="table",
        indexes=[
            {'columns': ['deployment_sfid']},
            {'columns': ['reporting_month']}
        ]
    )
}}

with deployments as (
    select
        d.deployment_id as deployment_ulid,
        d.salesforce_id as deployment_sfid,
        d.contract_id as contract_sfid,
        d.stage_name as deployment_status,
        a.account_id as account_sfid,
        a.account_name as account_name
    from {{ ref('fct_crm__deployment') }} d
    left join {{ ref('dim_crm__account') }} a on d.account_id = a.account_id
), contract_deployment_history as (
    select
        h.salesforce_deployment_id as deployment_sfid,
        c.contract_id as contract_sfid,
        c.contract_number as contract_number,
        c.status as contract_status,
        c.commit_amount as commit_amount,
        c.commit_type as commit_type,
        h.reporting_start_month,
        h.reporting_end_month,
        c.contract_start_date,
        c.contract_end_date,
        a.account_name as account_name,
        a.account_id as account_sfid
    from {{ ref('fct_crm__contract') }} c
    left join {{ ref('fct_crm__contract_deployment_history') }} h on c.contract_id = h.contract_id
    left join {{ ref('dim_crm__account') }} a on c.account_id = a.account_id
), infrastructure_elements as (
    select
        c.name as ie_cluster_name,
        p.name as ie_project_name,
        t.name as ie_table_name,
        t.cluster_project_table__c as cluster_project_table_name,
        concat(c.name, '-', p.name) as cluster_project_name,
        p.deployment__c as deployment_sfid,
        t.id as ie_table_sfid,
        p.id as ie_project_sfid,
        c.id as ie_cluster_sfid,
        p.stage__c as project_status,
        c.stage__c as cluster_status,
        t.last_sync_date__c as table_last_sync_date,
        p.last_sync_date__c as project_last_sync_date,
        c.last_sync_date__c as cluster_last_sync_date,
        c.hdx_shared_cluster__c as is_hdx_shared_cluster,
        c.multi_tenant_cluster__c as is_multi_tenant_cluster,
        t.table_uuid__c as table_uuid,
        p.project_uuid__c as project_uuid
    from raw_salesforce.ie_table__c t
    left join raw_salesforce.ie_project__c p on t.ie_project__c = p.id
    left join raw_salesforce.ie_cluster__c c on p.ie_cluster__c = c.id
    where t.is_deleted is False
    and p.is_deleted is False
    and c.is_deleted is False
), monthly_usage as (
    select
        date_trunc('month', date) as reporting_month,
        concat(cluster_hostname, '-', project_name, '-', table_name) as cluster_project_table_name,
        sum(total_bytes) as total_bytes,
        sum(total_rows) as total_rows
    from {{ source('argus', 'daily_usage_with_table') }}
    where date >= '2025-01-01'
    group by 1, 2
), ie_with_usage as (
    select
        ie.cluster_project_table_name,
        u.reporting_month::date as reporting_month,
        coalesce(c.account_name, d.account_name, 'N/A') as account_name,
        coalesce(c.contract_number, 'N/A') as contract_number,
        coalesce(d.deployment_ulid, 'N/A') as deployment_ulid,
        coalesce(ie.ie_cluster_name, 'N/A') as cluster_name,
        coalesce(ie.ie_project_name, 'N/A') as project_name,
        coalesce(ie.cluster_project_name, 'N/A') as cluster_project_name,
        case
            when ie.is_hdx_shared_cluster or ie.is_multi_tenant_cluster then coalesce(ie.cluster_project_name, 'N/A')
            else coalesce(ie.ie_cluster_name, 'N/A')
        end as infrastructure_name,
        coalesce(ie.ie_table_name, 'N/A') as table_name,
        u.total_bytes,
        (u.total_bytes / (1000 ^ 3)) as total_gb,
        (u.total_bytes / (1024 ^ 3)) as total_gib,
        (u.total_bytes / (1000 ^ 4)) as total_tb,
        (u.total_bytes / (1024 ^ 4)) as total_tib,
        u.total_rows,
        c.commit_amount,
        c.commit_type,
        case
            when c.commit_type = 'GB per Month'    then (u.total_bytes / (1000 ^ 3)) / nullif(c.commit_amount, 0)
            when c.commit_type = 'GiB per Month'   then (u.total_bytes / (1024 ^ 3)) / nullif(c.commit_amount, 0)
            when c.commit_type = 'TB per Month'    then (u.total_bytes / (1000 ^ 4)) / nullif(c.commit_amount, 0)
            when c.commit_type = 'TiB per Month'   then (u.total_bytes / (1024 ^ 4)) / nullif(c.commit_amount, 0)
            else                                        (u.total_rows / (1000 ^ 3)) / nullif(c.commit_amount, 0)
        end as pct_of_commit,
        coalesce(c.contract_start_date, '2000-01-01') as contract_start_date,
        coalesce(c.contract_end_date, '9999-01-01') as contract_end_date,
        coalesce(c.contract_status, 'N/A') as contract_status,
        coalesce(d.deployment_status, 'N/A') as deployment_status,
        coalesce(ie.cluster_status, 'N/A') as cluster_status,
        coalesce(ie.project_status, 'N/A') as project_status,
        ie.is_hdx_shared_cluster,
        ie.is_multi_tenant_cluster,
        (ie.is_hdx_shared_cluster or ie.is_multi_tenant_cluster) as is_multi_deployment_cluster,
        coalesce(c.account_sfid, 'N/A') as account_sfid,
        coalesce(c.contract_sfid, 'N/A') as contract_sfid,
        coalesce(d.deployment_sfid, 'N/A') as deployment_sfid,
        coalesce(ie.ie_cluster_sfid, 'N/A') as ie_cluster_sfid,
        coalesce(ie.ie_project_sfid, 'N/A') as ie_project_sfid,
        coalesce(ie.ie_table_sfid, 'N/A') as ie_table_sfid,
        coalesce(ie.cluster_last_sync_date) as ie_cluster_last_sync_date,
        coalesce(ie.project_last_sync_date) as ie_project_last_sync_date,
        coalesce(ie.table_last_sync_date) as ie_table_last_sync_date,
        coalesce(ie.project_uuid, 'N/A') as project_uuid,
        coalesce(ie.table_uuid, 'N/A') as table_uuid
    from infrastructure_elements ie
    left join monthly_usage u on ie.cluster_project_table_name = u.cluster_project_table_name
    left join deployments d on ie.deployment_sfid = d.deployment_sfid
    left join contract_deployment_history c
        on d.deployment_sfid = c.deployment_sfid
        and c.reporting_start_month <= u.reporting_month
        and c.reporting_end_month >= u.reporting_month
), final_data as (
    select
        ie.*
    from ie_with_usage ie
)
select * from final_data
