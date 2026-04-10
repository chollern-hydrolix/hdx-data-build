{{ config(materialized="table") }}

with deployments as (
    select * from {{ref('fct_crm__deployment')}}
), history as (
    select * from {{ref('fct_crm__contract_deployment_history')}}
), contracts as (
    select * from {{ref('fct_crm__contract')}}
), accounts as (
    select * from {{ref('dim_crm__account')}}
    where account_name not in ('123TEST', '456TEST')
), deployments_with_contracts as (
    select
        a.account_name,
        d.deployment_id,
        c.contract_number,
        c.contract_start_date,
        c.contract_end_date,
        c.reporting_start_date,
        c.reporting_end_date,
        c.tb_per_month_standard,
        c.tb_per_month_premium,
        c.standard_overages_per_gb,
        c.premium_overages_per_gb,
        c.raw_log_retention_standard,
        c.summary_log_retention_standard,
        c.commit_amount,
        c.commit_type,
        a.account_id,
        d.salesforce_id,
        c.contract_id
    from deployments d
    left join history h on d.salesforce_id = h.salesforce_deployment_id
    left join contracts c on h.contract_id = c.contract_id
    left join accounts a on d.account_id = a.account_id
)
select * from deployments_with_contracts order by account_name, contract_start_date, contract_id
