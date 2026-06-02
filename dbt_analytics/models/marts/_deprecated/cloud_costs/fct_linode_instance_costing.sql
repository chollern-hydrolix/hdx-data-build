{{ config(materialized="table") }}

/*
    This model is a slight modification of stg_linode_instance_billing, with the removal of the "invoice_month" dimension
*/

with linodes as (
    select
        cluster_id,
        cluster_created_date,
        cluster_label,
        cluster_region,
        linode_id,
        linode_created_date,
        linode_label,
        linode_final_status,
        linode_type_label,
        linode_type_list_price,
        linode_type_hdx_price,
        -- Use next month if linode_shutdown_date is null
        coalesce(
            linode_shutdown_date,
            (date_trunc('month', current_date) + interval '1 month')::date
        ) as linode_shutdown_date
    from {{ ref('stg_linode_instance_with_shutdown') }}
-- Add a created and shutdown month column based on date columns
), linodes_with_billing_period as (
    select
        *,
        date_trunc('month', linode_created_date)::date as linode_created_month,
        date_trunc('month', linode_shutdown_date)::date as linode_shutdown_month
    from linodes
), linodes_with_billing_intervals as (
    select
        l.*,
        l.linode_created_date as billing_interval_start_timestamp,
        l.linode_shutdown_date as billing_interval_end_timestamp
    from linodes_with_billing_period l
-- Add billable hours
), linodes_with_billable_hours as (
    select
        l.*,
        (extract(epoch FROM billing_interval_end_timestamp - billing_interval_start_timestamp) / 3600) AS billable_hours
    from linodes_with_billing_intervals l
-- Add costs
), linodes_with_estimated_costs as (
    select
        l.*,
        (linode_type_list_price * billable_hours) as total_amount
    from linodes_with_billable_hours l
)
select * from linodes_with_estimated_costs
