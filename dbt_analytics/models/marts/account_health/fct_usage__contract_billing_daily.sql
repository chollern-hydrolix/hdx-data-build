{{
    config(
        materialized='incremental',
        unique_key=['contract_id', 'deployment_sfid', 'billing_group', 'reporting_date'],
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['reporting_date']},
            {'columns': ['contract_id']},
            {'columns': ['account_id']},
            {'columns': ['deployment_sfid']}
        ]
    )
}}

/*
===============================================================================
Model:
    fct_usage__contract_billing_daily

Grain:
    One row per (contract_id, deployment_sfid, billing_group, reporting_date)

Description:
    Daily usage sourced from argus.daily_usage_with_table, mapped to the
    commercially active contract via fct_crm__contract_deployment_history.
    Tables are linked to deployments through the Salesforce IE hierarchy
    (ie_table__c -> ie_project__c -> deployment).

    Breaks usage into Premium vs Standard via ie_table__c.billing_group__c
    and the contract's premium_overages__c: Premium billing_group rows are
    only classified as Premium when the contract has premium_overages__c > 0;
    otherwise they are grouped with Standard.

    Tables with billing_group = 'Omitted' are excluded.

TODO: Add premium_overages__c to fct_crm__contract to eliminate the
      raw_salesforce.contract join in contract_billing_config.
===============================================================================
*/

with ie_tables as (
    -- Deduplicate on cluster_project_table key; keep the most recently synced record.
    select
        cluster_project_table__c as cluster_project_table,
        billing_group__c         as billing_group,
        id                       as ie_table_id,
        ie_project__c            as ie_project_id
    from (
        select
            *,
            row_number() over (
                partition by cluster_project_table__c
                order by last_sync_date__c desc, id desc
            ) as _rank
        from {{ source('raw_salesforce', 'ie_table__c') }}
        where is_deleted is False
          and billing_group__c != 'Omitted'
    ) ranked
    where _rank = 1
), ie_project_to_deployment as (
    select
        id            as ie_project_id,
        deployment__c as deployment_sfid
    from {{ source('raw_salesforce', 'ie_project__c') }}
    where is_deleted is False
      and deployment__c is not null
), raw_usage as (
    select
        concat(cluster_hostname, '-', project_name, '-', table_name) as cluster_project_table,
        date::date                                                     as reporting_date,
        sum(total_bytes)                                               as total_bytes
    from {{ source('argus', 'daily_usage_with_table') }}
    {% if is_incremental() %}
    where date::date > (select coalesce(max(reporting_date), '1900-01-01'::date) from {{ this }})
    {% endif %}
    group by 1, 2
), usage_with_table_context as (
    select
        u.reporting_date,
        u.total_bytes,
        t.billing_group,
        p.deployment_sfid
    from raw_usage u
    inner join ie_tables t
        on u.cluster_project_table = t.cluster_project_table
    inner join ie_project_to_deployment p
        on t.ie_project_id = p.ie_project_id
), usage_with_contract as (
    select
        u.reporting_date,
        u.total_bytes,
        u.billing_group,
        u.deployment_sfid,
        cdh.contract_id
    from usage_with_table_context u
    inner join {{ ref('fct_crm__contract_deployment_history') }} cdh
        on u.deployment_sfid = cdh.salesforce_deployment_id
       and u.reporting_date >= cdh.reporting_start_date
       and u.reporting_date <= cdh.reporting_end_date
), usage_aggregated as (
    select
        contract_id,
        deployment_sfid,
        billing_group,
        reporting_date,
        sum(total_bytes) as total_bytes
    from usage_with_contract
    group by 1, 2, 3, 4
), contract_billing_config as (
    -- Pulls premium_overages__c which is not yet in fct_crm__contract.
    -- Once added there, replace this CTE with a column on the contract join below.
    select
        id                                  as contract_id,
        coalesce(premium_overages__c, 0)    as premium_overages
    from {{ source('raw_salesforce', 'contract') }}
    where is_deleted is False
)
select
    u.contract_id,
    u.deployment_sfid,
    u.billing_group,
    u.reporting_date,
    date_trunc('month', u.reporting_date)::date as reporting_month,
    -- Premium when billing_group = 'Premium' AND the contract has premium overage pricing;
    -- otherwise grouped with Standard (matches Akamai billing API logic).
    case
        when u.billing_group = 'Premium' and coalesce(bc.premium_overages, 0) > 0
        then 'Premium'
        else 'Standard'
    end as usage_type,
    -- account dimensions
    a.account_id,
    a.account_name,
    a.parent_account_name,
    -- contract dimensions
    c.contract_number,
    c.akamai_contract_id,
    c.contract_start_date,
    c.contract_end_date,
    c.status                                    as contract_status,
    c.type_reporting,
    c.hydrolix_product,
    c.region,
    c.commit_type,
    c.commit_amount,
    c.mrr_gross,
    -- usage metrics
    u.total_bytes,
    u.total_bytes / (1000.0 ^ 3)               as total_gb,
    u.total_bytes / (1024.0 ^ 3)               as total_gib,
    u.total_bytes / (1000.0 ^ 4)               as total_tb,
    u.total_bytes / (1024.0 ^ 4)               as total_tib
from usage_aggregated u
left join {{ ref('fct_crm__contract') }} c  on u.contract_id = c.contract_id
left join {{ ref('dim_crm__account') }} a   on c.account_id = a.account_id
left join contract_billing_config bc        on u.contract_id = bc.contract_id