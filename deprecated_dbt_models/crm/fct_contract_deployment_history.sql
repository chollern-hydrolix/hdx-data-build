{{ config(materialized="table") }}

with recursive deployment_with_latest_contract as (
    select
        deployment_id,
        contract_id
    from {{ref('fct_deployment')}}
    where contract_id is not null
), contract_loop as (
    select
        c.contract_id,
        c.contract_number,
        c.previous_contract_id,
        d.deployment_id
    from {{ref('fct_contract')}} c
    left join deployment_with_latest_contract d on c.contract_id = d.contract_id
    where c.replaced_by_new_contract is False
    and status = 'Activated'

    union all

    select
        c.contract_id,
        c.contract_number,
        c.previous_contract_id,
        cl.deployment_id
    from {{ref('fct_contract')}} c
    join contract_loop cl on c.contract_id = cl.previous_contract_id
)
select * from contract_loop
