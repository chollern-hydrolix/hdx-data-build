{{
    config(
        materialized="table",
        indexes=[
            {'columns': ['linode_id']},
            {'columns': ['reporting_date']}
        ]
    )
}}

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
        ) as linode_shutdown_date,
        linode_type_label ilike '%premium%' as is_premium,
        cloud_account
    from {{ ref('stg_linode_instance_with_shutdown') }}
-- Add a created and shutdown month column based on date columns
), linodes_with_billing_period as (
    select
        *,
        date_trunc('day', linode_created_date)::date as linode_created_date_only,
        date_trunc('day', linode_shutdown_date)::date as linode_shutdown_date_only
    from linodes
), days as (
    select
        day_date,
        month_date
    from {{ ref('dim_day') }}
    where day_date between '2025-01-01' and CURRENT_DATE
-- Inner join on months to get a row for every month that an instance was active (this enables invoice month filtering)
), linodes_with_billing_period_range as (
    select
        l.*,
        d.day_date as reporting_date,
        d.month_date as reporting_month,
        date_trunc('month', current_date) as current_month,
        date_trunc('month', current_date) = d.month_date as is_current_month
    from linodes_with_billing_period l
    inner join days d
        on l.linode_created_date_only <= d.day_date
        and l.linode_shutdown_date_only >= d.day_date
-- Add billing start and end dates used to calculated quantity of hours utilized
), linodes_with_billing_intervals as (
    select
        l.*,
        case
            when l.linode_created_date_only < l.reporting_date then l.reporting_date at time zone 'UTC'
            else l.linode_created_date
        end as billing_interval_start_timestamp,
        case
            when l.linode_shutdown_date_only > l.reporting_date then (l.reporting_date + interval '1 day') at time zone 'UTC'
            else l.linode_shutdown_date
        end as billing_interval_end_timestamp
    from linodes_with_billing_period_range l
-- Add billable hours
), linodes_with_billable_hours as (
    select
        l.*,
        ceil((extract(epoch FROM (reporting_month + interval '1 month') - reporting_month) / 3600)) as months_hours,
        ceil((extract(epoch FROM billing_interval_end_timestamp - billing_interval_start_timestamp) / 3600)) AS billable_hours
    from linodes_with_billing_intervals l
-- Group by month to see if the Linode Instance spans the entire month
), linodes_grouped_by_month as (
    select
        linode_id,
        reporting_month,
        sum(billable_hours) as full_month_billable_hours
    from linodes_with_billable_hours
    group by 1, 2
-- Add costs
), linodes_with_estimated_raw_cost as (
    select
        l.*,
        case
            when l.months_hours = lgm.full_month_billable_hours then l.linode_type_monthly_list_price / nullif(l.months_hours, 0) * 24
            else l.linode_type_list_price * l.billable_hours
        end as total_amount
        -- case
        --     when l.months_hours = lgm.full_month_billable_hours then
        --         case
        --             when l.is_current_month then
        --                 case
        --                     when l.is_premium then (l.linode_type_hdx_price * l.billable_hours * 0.94)
        --                     else (l.linode_type_hdx_price * l.billable_hours * 0.94)
        --                 end
        --             else
        --                 case
        --                     when l.is_premium then l.linode_type_monthly_list_price / nullif(l.months_hours, 0) * 24 * 0.58
        --                     else l.linode_type_monthly_list_price / nullif(l.months_hours, 0) * 24 * 0.7
        --                 end
        --         end
        --     else (l.linode_type_hdx_price * l.billable_hours)
        -- end as hdx_amount
    from linodes_with_billable_hours l
    left join linodes_grouped_by_month lgm
        on l.linode_id = lgm.linode_id
        and l.reporting_month = lgm.reporting_month
), linodes_with_premium_discount_cost as (
    select
        l.*,
        case
            when l.is_premium then l.total_amount * 0.8324
            else l.total_amount
        end as premium_discount_amount
    from linodes_with_estimated_raw_cost l
), linodes_with_hdx_cost as (
    select
        l.*,
        (l.premium_discount_amount * 0.7) as hdx_amount
    from linodes_with_premium_discount_cost l
)
select * from linodes_with_hdx_cost
