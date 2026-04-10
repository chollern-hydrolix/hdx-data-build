{{ config(materialized="table") }}

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
        linode_type_monthly_list_price,
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
), months as (
    select * from {{ ref('dim_month') }}
    where month_date between '2025-01-01' and CURRENT_DATE
-- Inner join on months to get a row for every month that an instance was active (this enables invoice month filtering)
), linodes_with_billing_period_range as (
    select
        l.*,
        m.month_date as invoice_month
    from linodes_with_billing_period l
    inner join months m
        on l.linode_created_month <= m.month_date
        and l.linode_shutdown_month >= m.month_date
-- Add billing start and end dates used to calculated quantity of hours utilized
), linodes_with_billing_intervals as (
    select
        l.*,
        case
            when l.linode_created_month < l.invoice_month then l.invoice_month at time zone 'UTC'
            else l.linode_created_date
        end as billing_interval_start_timestamp,
        case
            when l.linode_shutdown_month > l.invoice_month then (date_trunc('month', l.invoice_month) + interval '1 month') at time zone 'UTC'
            else l.linode_shutdown_date
        end as billing_interval_end_timestamp
    from linodes_with_billing_period_range l
-- Add billable hours
), linodes_with_billable_hours as (
    select
        l.*,
        ceil((extract(epoch FROM (invoice_month + interval '1 month') - invoice_month) / 3600)) as months_hours,
        ceil((extract(epoch FROM billing_interval_end_timestamp - billing_interval_start_timestamp) / 3600)) AS billable_hours
    from linodes_with_billing_intervals l
-- Add costs
), linodes_with_estimated_costs as (
    select
        l.*,
        case
            when months_hours = billable_hours then linode_type_monthly_list_price
            else linode_type_list_price * billable_hours
        end as total_amount,
        -- (linode_type_list_price * billable_hours) as total_amount,
        (linode_type_hdx_price * billable_hours) as hdx_amount
    from linodes_with_billable_hours l
)
select * from linodes_with_estimated_costs
