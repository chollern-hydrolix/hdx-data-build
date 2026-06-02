{{ config(materialized='table') }}

/*
Grain: (resource_group, resource_name)

Maps Azure resource groups to the resource names (storage accounts / buckets)
they contain, with a cost_ratio that captures each resource_name's share of its
resource_group total in the most recent closed billing month.

cost_ratio is used by fct_cogs__azure_bucket_cost to split daily_usage_fact
(resource_group grain) into resource_name-level estimates for the current month.
*/

with latest_month as (
    select max(billing_month) as billing_month
    from azure.monthly_cost_fact
), resource_name_costs as (
    select
        resource_group,
        resource_name,
        sum(pre_tax_cost) as resource_name_cost
    from azure.monthly_cost_fact
    inner join latest_month using (billing_month)
    group by 1, 2
), resource_group_totals as (
    select
        resource_group,
        sum(resource_name_cost) as resource_group_cost
    from resource_name_costs
    group by 1
)
select
    rn.resource_group,
    rn.resource_name,
    rn.resource_name_cost / nullif(rgt.resource_group_cost, 0) as cost_ratio
from resource_name_costs rn
join resource_group_totals rgt on rn.resource_group = rgt.resource_group