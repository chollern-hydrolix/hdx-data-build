{{
    config(
        materialized="table"
    )
}}

/*

Joins Akamai costs with Salesforce Deployments

Note: this model is not necessarily unique at the deployment and month intersection, there can be repeated Deployment IDs for a given Month.

*/

with raw_akamai_invoice as (
    select
        date_trunc('month', invoice_publish_month - interval '1 month')::date as invoice_month,
        cloud_account,
        case when cluster_label is not null then concat(cluster_label, '.trafficpeak.live') else 'N/A' end as cluster_hostname,
        sum(total) as raw_linode_cost,
        sum(premium_discount_total) as raw_premium_discount_linode_cost,
        sum(hdx_total) as raw_hdx_linode_cost
    from {{ ref('fct_akm__invoice_item') }}
    where total >= 0
    group by 1, 2, 3
), akamai_invoice_allocation as (
    select
        i.invoice_month,
        i.cluster_hostname,
        coalesce(alc.cluster_project_name, 'N/A') as cluster_project_name,
        i.raw_linode_cost,
        i.raw_premium_discount_linode_cost,
        i.raw_hdx_linode_cost,
        coalesce(alc.monthly_pro_rated_pct, 1) as pro_rated_pct
    from raw_akamai_invoice i
    left join {{ ref('stg_linode__monthly_shared_cluster_allocation') }} alc
        on i.invoice_month = alc.month
        and i.cluster_hostname = alc.cluster_hostname
    where invoice_month >= '2025-01-01'
), ie_clusters as (
    select
        name as cluster_hostname,
        deployment__c as deployment_sfid
    from {{ source('raw_salesforce', 'ie_cluster__c') }}
    where is_deleted is False
), ie_projects as (
    select
        cluster_project__c as cluster_project_name,
        deployment__c as deployment_sfid
    from {{ source('raw_salesforce', 'ie_project__c') }}
    where is_deleted is False
), akamai_invoice_with_deployment as (
    select
        a.*,
        case
            when a.cluster_project_name = 'N/A' then ie_c.deployment_sfid
            else ie_p.deployment_sfid
        end as deployment_sfid
    from akamai_invoice_allocation a
    -- Dedicated Deployment
    left join ie_clusters ie_c on a.cluster_hostname = ie_c.cluster_hostname
    -- Shared Deployment
    left join ie_projects ie_p on a.cluster_project_name = ie_p.cluster_project_name
), akamai_invoice_allocated_costs as (
    select
        a.*,
        (raw_linode_cost * pro_rated_pct) as total_linode_cost,
        (raw_premium_discount_linode_cost * pro_rated_pct) as premium_discount_linode_cost,
        (raw_hdx_linode_cost * pro_rated_pct) as hdx_linode_cost
    from akamai_invoice_with_deployment a
)
select * from akamai_invoice_allocated_costs
