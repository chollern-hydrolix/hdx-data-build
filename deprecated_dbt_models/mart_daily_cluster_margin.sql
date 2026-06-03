{{ config(materialized="table") }}

with daily_contract_margins as (
    select
        cluster_label,
        reporting_date,
        reporting_month,
        daily_mrr_gross,
        daily_mrr_net,
        total_azure_cost,
        total_linode_cost,
        total_costs
    from {{ref('mart_daily_contract_margin')}}
), daily_cluster_mrr_and_costs as (
    select
        cluster_label,
        reporting_date,
        reporting_month,
        sum(daily_mrr_gross) daily_mrr_gross,
        sum(daily_mrr_net) daily_mrr_net,
        sum(total_azure_cost) as total_azure_cost,
        sum(total_linode_cost) as total_linode_cost,
        sum(total_costs) as total_costs
    from daily_contract_margins
    group by 1, 2, 3
), linode_costs_grouped as (
    select
        cluster_label,
        reporting_date,
        sum(hdx_amount) as hdx_amount
    from {{ref('stg_daily_linode_resource_billing')}}
    group by 1, 2
), daily_cluster_mrr_linode_costs as (
    select
        d.cluster_label,
        d.reporting_date,
        d.reporting_month,
        d.daily_mrr_gross,
        d.daily_mrr_net,
        d.total_azure_cost,
        round(coalesce(l.hdx_amount, 0)::numeric, 2) as total_linode_cost
    from daily_cluster_mrr_and_costs d
    left join linode_costs_grouped l
        on d.cluster_label = l.cluster_label
        and d.reporting_date = l.reporting_date
), azure_usage_aggregated as (
    -- Azure Usage by Resource Group by Day
    select
        resource_group,
        usage_date,
        sum(pre_tax_cost) as pre_tax_cost
    from azure.daily_usage_fact
    group by 1, 2
), daily_cluster_mrr_azure_costs as (
    select
        d.cluster_label,
        d.reporting_date,
        d.reporting_month,
        d.daily_mrr_gross,
        d.daily_mrr_net,
        case
            when sc.region is null then d.total_azure_cost
            else pre_tax_cost
        end as total_azure_cost,
        d.total_linode_cost
    from daily_cluster_mrr_linode_costs d
    left join azure_usage_aggregated a
        on d.cluster_label = a.resource_group
        and d.reporting_date = a.usage_date
    left join static_mapping.shared_cluster sc on d.cluster_label = sc.region
), daily_cluster_with_total_costs as (
    select
        *,
        (total_azure_cost + total_linode_cost) as total_costs
    from daily_cluster_mrr_azure_costs
), daily_cluster_with_margin as (
    select
        *,
        (daily_mrr_gross - total_azure_cost - total_linode_cost) as gross_mrr_margin,
        (daily_mrr_net - total_azure_cost - total_linode_cost) as net_mrr_margin
    from daily_cluster_with_total_costs
), daily_cluster_with_pct as (
    select
        *,
        round((gross_mrr_margin::numeric / nullif(daily_mrr_gross::numeric, 0)), 6) as gross_mrr_margin_pct,
        round((net_mrr_margin::numeric / nullif(daily_mrr_net::numeric, 0)), 6) as net_mrr_margin_pct
    from daily_cluster_with_margin
)
select * from daily_cluster_with_pct
