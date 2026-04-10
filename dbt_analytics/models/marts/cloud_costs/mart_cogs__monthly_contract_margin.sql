{{
    config(
        materialized="table"
    )
}}

with months as (
    select
        month_date
    from {{ ref('dim_month') }}
    where month_date between '2025-01-01' and CURRENT_DATE
), contract_deployment_history as (
    select
        cdh.contract_id,
        cdh.salesforce_deployment_id,
        cdh.contract_start_date,
        cdh.contract_end_date,
        cdh.reporting_start_date,
        cdh.reporting_end_date,
        cdh.reporting_start_month,
        cdh.reporting_end_month,
        d.deployment_id as deployment_ulid,
        c.region,
        c.hydrolix_product,
        c.commit_type,
        c.commit_amount,
        a.account_id,
        a.account_name
    from {{ ref('fct_crm__contract_deployment_history') }} cdh
    left join {{ ref('fct_crm__contract') }} c on cdh.contract_id = c.contract_id
    left join {{ ref('fct_crm__deployment') }} d on cdh.salesforce_deployment_id = d.salesforce_id
    left join {{ ref('dim_crm__account') }} a on c.account_id = a.account_id
), contracts_with_months as (
    select
        m.month_date as reporting_month,
        c.account_name,
        c.region,
        c.contract_start_date,
        c.contract_end_date,
        c.hydrolix_product,
        c.commit_amount,
        c.commit_type,
        c.salesforce_deployment_id as deployment_sfid,
        c.deployment_ulid,
        c.contract_id,
        c.account_id
    from contract_deployment_history c
    left join months m
        on c.reporting_start_month <= m.month_date
        and c.reporting_end_month >= m.month_date
), mrr_by_contract_by_month as (
    select
        reporting_month,
        contract_id,
        ending_mrr_gross,
        total_bytes,
        total_rows
    from {{ ref('mart_mrr_contracts') }}
), akamai_invoice_costs_by_deployment as (
    select
        invoice_month,
        deployment_sfid,
        sum(total_linode_cost) as total_linode_cost,
        sum(premium_discount_linode_cost) as premium_discount_linode_cost,
        sum(hdx_linode_cost) as hdx_linode_cost
    from {{ ref('fct_cogs__akamai_deployment_cost') }}
    group by 1, 2
), azure_bucket_costs_by_deployment as (
    select
        invoice_month,
        deployment_sfid,
        sum(azure_cost) as azure_bucket_cost
    from {{ ref('fct_cogs__azure_bucket_cost') }}
    group by 1, 2
), contracts_with_mrr_and_costs as (
    select
        c.*,
        mrr.ending_mrr_gross,
        akm.total_linode_cost,
        akm.premium_discount_linode_cost,
        akm.hdx_linode_cost,
        coalesce(azr_bkt.azure_bucket_cost, 0) as azure_bucket_cost,
        mrr.total_bytes,
        mrr.total_rows
    from contracts_with_months c
    left join akamai_invoice_costs_by_deployment akm
        on c.reporting_month = akm.invoice_month
        and c.deployment_sfid = akm.deployment_sfid
    left join azure_bucket_costs_by_deployment azr_bkt
        on c.reporting_month = azr_bkt.invoice_month
        and c.deployment_sfid = azr_bkt.deployment_sfid
    left join mrr_by_contract_by_month mrr
        on c.reporting_month = mrr.reporting_month
        and c.contract_id = mrr.contract_id
)
select * from contracts_with_mrr_and_costs

-- where deployment_sfid = 'a0LU10000037RBRMA2'
-- order by reporting_month


-- select
--     reporting_month,
--     deployment_sfid,
--     count(*)
-- from contracts_with_mrr_and_costs
-- where reporting_month is not null
-- group by 1, 2
-- having count(*) > 1
