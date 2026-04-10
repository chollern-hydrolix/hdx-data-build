{{ config(materialized="table") }}

select * from {{ ref('fct_active_contract') }}
where is_expiration_risk is True
