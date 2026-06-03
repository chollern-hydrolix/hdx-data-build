{{
    config(materialized="table")
}}

with deployments as (
    select
        a.account_name,
        d.hydrolix_product,
        d.cluster_hostname,
        d.hdx_deployment_name,
        d.cluster_project_name_calculated,
        d.cluster_type_calculated,
        c.contract_number,
        d.poc,
        d.last_verified,
        d.deployment_id,
        d.opportunity_id
    from {{ref('fct_deployment')}} d
    left join {{ref('dim_account')}} a on d.account_id = a.account_id
    left join {{ref('fct_contract')}} c on d.contract_id = c.contract_id
), usage_by_deployment_name as (
    select True
), deployment_with_usage as (
    select
        d.*
    from deployments d
    -- left join usage_by_deployment_name u on d.name = u.name
), deployments_with_calculated_fields as (
    select
        d.*,
        case
            when d.last_verified >= current_date - interval '7 days' then 'Active'  -- TODO: avg_daily_usage_last_7_days > 0
            else 'Inactive'
        end as cluster_status
    from deployment_with_usage d
)
select * from deployments_with_calculated_fields
