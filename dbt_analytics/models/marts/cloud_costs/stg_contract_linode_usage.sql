{{ config(materialized="view") }}

/*
    Grain: Contract x Month
    Measure: Usage
*/

with deployments_by_contract as (
    -- Deployments with Contract
    select
        cdh.contract_id,
        d.deployment_id,
        d.hdx_deployment_name,
        d.cluster_hostname,
        c.contract_start_month,
        c.contract_start_date,
        c.contract_end_date,
        d.last_verified
    from {{ref('fct_deployment')}} d
    -- Join to get every contract that has ever been associated with a particular deployment
    left join {{ref('fct_contract_deployment_history')}} cdh on d.deployment_id = cdh.deployment_id
    -- Join to get additional contract data
    left join {{ref('fct_contract')}} c on cdh.contract_id = c.contract_id
    where d.contract_id is not null
), deployments_ranked_by_recency as (
    select
        *,
        rank() over (
            partition by contract_id, hdx_deployment_name
            order by last_verified desc
        ) as recency_rank
    from deployments_by_contract
), primary_deployments_by_contract as (
    select
        contract_id,
        deployment_id,
        hdx_deployment_name,
        cluster_hostname,
        contract_start_month,
        contract_start_date,
        contract_end_date
    from deployments_ranked_by_recency
    where recency_rank = 1
), linode_usage_aggregated as (
    -- Aggregated Linode usage by Cluster and Invoice Month
    select
        cluster_label,
        invoice_month,
        sum(hdx_amount) as total_amount
    from {{ref('stg_linode_instance_billing')}}
    group by 1, 2
), shared_instance_usage as (
    select
        c.hostname,
        u.invoice_month,
        u.total_amount
    from linode_usage_aggregated u
    inner join static_mapping.shared_cluster c on u.cluster_label = c.region
), deployments_with_linode_usage as (
    select
        d.*,
        coalesce(l.invoice_month, e.month) as usage_month,
        case
            when l.total_amount is not null then l.total_amount
            else coalesce(s.total_amount * e.pct_usage, 0)
        end as total_amount
    from primary_deployments_by_contract d
    -- Get linode usage for dedicated instances
    left join linode_usage_aggregated l on d.hdx_deployment_name = l.cluster_label
    -- Get linode usage pct for shared instances
    left join {{ref('stg_shared_cluster_project_usage_estimate_pct')}} e
        on d.hdx_deployment_name = e.merge_name
        and d.contract_start_date <= e.month
        and d.contract_end_date >= e.month
    left join shared_instance_usage s
        on d.cluster_hostname = s.hostname
        and e.month = s.invoice_month
), deployments_grouped as (
    select
        contract_id,
        usage_month as invoice_month,
        sum(total_amount) as total_amount
    from deployments_with_linode_usage
    where usage_month is not null
    group by 1, 2
)
select * from deployments_grouped
