{{ config(materialized="table") }}

with poc_data as (
    select
        *
    from {{ref('fct_hydrolix_poc')}}
), poc_with_deployment as (
    select
        coalesce(a.account_name, p.customer_name) as account_name,
        p.hdx_deployment_id,
        d.hdx_deployment_name,
        p.approval_amount,
        p.status as poc_status,
        p.credit_amount,
        p.poc_month,
        p.requested_date,
        p.start_date,
        p.end_date,
        p.invoice_number,
        p.date_applied,
        p.akamai_account_id,
        d.cluster_hostname,
        d.cluster_project_name,
        d.status as deployment_status,
        d.type,
        d.sales_region,
        d.cost_type,
        d.hydrolix_product,
        d.cluster_cloud,
        d.cluster_label
    from poc_data p
    left join {{ref('fct_deployment')}} d on p.hdx_deployment_id = d.deployment_id
    left join {{ref('dim_account')}} a on d.account_id = a.account_id
)
select * from poc_with_deployment
