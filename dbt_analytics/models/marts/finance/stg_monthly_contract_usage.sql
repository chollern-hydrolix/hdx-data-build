{{ config(materialized="view") }}

/*
    A simple model to aggregate Argus usage by Contract
    This model does not take into account contract start and end dates
*/

with argus_usage as (
    select
        deployment_id,
        date_trunc('month', date)::date as reporting_month,
        sum(total_rows) as total_rows,
        sum(total_bytes) as total_bytes
    from argus.daily_usage
    group by 1, 2
), contract_with_monthly_usage as (
    select
        c.contract_id,
        contract.commit_type,
        u.reporting_month,
        sum(u.total_rows) as total_rows,
        sum(u.total_bytes) as total_bytes
    from {{ref('fct_contract_deployment_history')}} c
    left join {{ref('fct_contract')}} contract on c.contract_id = contract.contract_id
    left join argus_usage u on c.deployment_id = u.deployment_id
    group by 1, 2, 3
)
select
    contract_id,
    reporting_month,
    sum(total_rows) as total_rows,
    sum(total_bytes) as total_bytes
from contract_with_monthly_usage
group by 1, 2
