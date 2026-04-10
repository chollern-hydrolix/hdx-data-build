{{config(materialized="table")}}

/* This needs to be materialized="table" for RETL reliability */

/*
===============================================================================
Model:
    fct_usage__deployment_daily

Grain:
    One row per (contract_id, deployment_id, reporting_date)

Description:
    Daily usage aligned to Salesforce deployments and active contracts.
    Maps infrastructure project usage to deployments via cluster/project
    relationships and time-bound contract deployment history.
    Ensures usage is commercially aligned and revenue-attributable.

Metrics:
    - total_bytes  (daily usage aggregated to deployment)
    - total_rows   (daily usage aggregated to deployment)
    - max_qpm      (daily peak query performance per deployment)
===============================================================================
*/

with usage_query_data as (
    select * from {{ref('fct_usage__project_daily')}}
), usage_query_with_deployment as (
    select
        u.*,
        case
            when ie_c.hdx_shared_cluster__c is True then ie_p.deployment__c
            else ie_c.deployment__c
        end as salesforce_id
    from usage_query_data u
    left join {{source('raw_salesforce', 'ie_project__c')}} ie_p on u.cluster_project_name = ie_p.cluster_project__c
    left join {{source('raw_salesforce', 'ie_cluster__c')}} ie_c on ie_p.ie_cluster__c = ie_c.id
), usage_grouped_by_deployment as (
    select
        salesforce_id,
        reporting_date,
        sum(total_rows) as total_rows,
        sum(total_bytes) as total_bytes,
        max(max_qpm) as max_qpm,
        sum(total_queries) as total_queries
    from usage_query_with_deployment
    group by 1, 2
), deployments_with_usage as (
    select
        d.salesforce_id,
        d.deployment_id,
        d.account_id,
        u.reporting_date,
        coalesce(u.total_rows, 0) as total_rows,
        coalesce(u.total_bytes, 0) as total_bytes,
        coalesce(u.max_qpm, 0) as max_qpm,
        coalesce(u.total_queries, 0) as total_queries
    from {{ ref('fct_crm__deployment') }} d
    left join usage_grouped_by_deployment u on d.salesforce_id = u.salesforce_id
), deployments_with_contract as (
    select
        d.*,
        cd_hist.contract_id
    from deployments_with_usage d
    left join {{ref('fct_crm__contract_deployment_history')}} cd_hist
        on d.salesforce_id = cd_hist.salesforce_deployment_id
        and d.reporting_date >= cd_hist.reporting_start_date
        and d.reporting_date <= cd_hist.reporting_end_date
), deployments_filtered as (
    select
        *
    from deployments_with_contract
    where contract_id is not null
)
select * from deployments_filtered
