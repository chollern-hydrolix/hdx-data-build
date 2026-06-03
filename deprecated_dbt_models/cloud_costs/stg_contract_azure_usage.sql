{{ config(materialized="view") }}

/*
    Grain: Contract x Day
    Measure: Usage
*/

with deployments_by_contract as (
    -- Deployments with Contract
    select
        contract_id,
        deployment_id
    from {{ref('fct_contract_deployment_history')}}
    where contract_id is not null
    -- select
    --     d.contract_id,
    --     d.deployment_id
    -- from {{ref('fct_deployment')}} d
    -- where d.contract_id is not null
), deployments_with_azure_resource_group as (
    -- Contract with HDX Deployment with Azure Resource Group Mapping
    select
        d.*,
        a.name as resource_group
    from deployments_by_contract d
    left join salesforce.azure_resource_group a on d.deployment_id = a.h_d_x_deployment__c
), azure_usage_aggregated as (
    -- Azure Usage by Resource Group by Day
    select
        resource_group,
        usage_date,
        sum(pre_tax_cost) as pre_tax_cost
    from azure.daily_usage_fact
    group by 1, 2
), deployments_with_azure_usage as (
    -- Contract with Azure Resource Group with actual usage
    select
        d.*,
        a.usage_date as azure_usage_date,
        a.pre_tax_cost as azure_cost
    from deployments_with_azure_resource_group d
    left join azure_usage_aggregated a on d.resource_group = a.resource_group
), deployments_grouped as (
    select
        contract_id,
        azure_usage_date,
        sum(azure_cost) as azure_cost
    from deployments_with_azure_usage
    group by 1, 2
)
select * from deployments_grouped
