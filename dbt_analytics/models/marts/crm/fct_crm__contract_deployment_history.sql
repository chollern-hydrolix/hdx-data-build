{{config(materialized="table")}}

with recursive deployment_with_latest_contract as (
    select distinct on (contract_id)
        salesforce_id as salesforce_deployment_id,
        deployment_id,
        contract_id
    from {{ref('fct_crm__deployment')}}
    where contract_id is not null
    order by contract_id, last_modified_date desc
), contract_loop as (
    select
        c.contract_id,
        c.contract_number,
        c.previous_contract_id,
        d.salesforce_deployment_id,
        d.deployment_id
    from {{ref('fct_crm__contract')}} c
    left join deployment_with_latest_contract d on c.contract_id = d.contract_id
    where c.replaced_by_new_contract is False
    and status = 'Activated'

    union all

    select
        c.contract_id,
        c.contract_number,
        c.previous_contract_id,
        cl.salesforce_deployment_id,
        cl.deployment_id
    from {{ref('fct_crm__contract')}} c
    join contract_loop cl on c.contract_id = cl.previous_contract_id
), contracts_with_metadata as (
    select
        cl.*,
        c.contract_start_date,
        c.contract_end_date,
        c.reporting_start_date,
        c.reporting_end_date
    from contract_loop cl
    left join {{ref('fct_crm__contract')}} c on cl.contract_id = c.contract_id
    where cl.salesforce_deployment_id is not null
), contracts_with_metadata_deduped as (
    select distinct on (contract_id)
        *
    from contracts_with_metadata
    order by contract_id
), contracts_with_reporting_months as (
    select
        c.*,
        case
            when c.previous_contract_id is null then date_trunc('month', c.reporting_start_date)::date
            else (date_trunc('month', prev_c.reporting_end_date) + interval '1 month')::date
        end as reporting_start_month,
        date_trunc('month', c.reporting_end_date)::date as reporting_end_month
    from contracts_with_metadata_deduped c
    left join contracts_with_metadata_deduped prev_c on c.previous_contract_id = prev_c.contract_id
)
select * from contracts_with_reporting_months

-- Duplicate rows in contracts_with_metadata
-- select contract_id, count(*) from contracts_with_metadata
-- group by 1 having count(*) > 1

-- Duplicate rows in final result
-- select contract_id from contracts_with_reporting_months
-- group by 1 having count(*) > 1
