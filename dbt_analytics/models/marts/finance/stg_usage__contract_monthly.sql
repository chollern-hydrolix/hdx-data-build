{{config(materialized="table")}}

/*
===============================================================================
Model:
    stg_usage__contract_monthly

Grain:
    One row per (contract_id, reporting_month)

Description:
    Monthly contract-level utilization derived from daily deployment usage.
    Expands contracts across valid reporting dates, calculates daily cumulative
    usage, and rolls metrics up to the month level. Supports burn tracking,
    pacing analysis, and contract utilization monitoring.

Metrics:
    - total_bytes            (monthly usage)
    - total_rows             (monthly usage)
    - max_qpm                (monthly peak performance)
    - cumulative_bytes       (lifetime bytes consumed through month-end)
    - cumulative_rows        (lifetime rows consumed through month-end)
    - cumulative_max_qpm     (lifetime peak performance through month-end)
===============================================================================
*/

with daily_contract_utilization as (
    -- Utilization grouped by contract and date
    select
        contract_id,
        reporting_date,
        date_trunc('month', reporting_date) as reporting_month,
        sum(total_bytes) as total_bytes,
        sum(total_rows) as total_rows,
        max(max_qpm) as max_qpm,
        sum(total_queries) as total_queries
    from {{ref('fct_usage__deployment_daily')}}
    group by 1, 2, 3
), days as (
    -- Day Dimension
    SELECT * FROM {{ ref('dim_day') }}
    WHERE day_date BETWEEN '2025-01-01' AND CURRENT_DATE
), contracts as (
    -- Contracts with start and end date
    select
        contract_id,
        reporting_start_date,
        reporting_end_date
    from {{ref('fct_crm__contract')}}
), contracts_with_days as (
    -- Contracts joined with the day dimension
    select
        c.*,
        d.day_date as reporting_date,
        d.month_date as reporting_month
    from contracts c
    inner join days d
        on c.reporting_start_date <= d.day_date
        and c.reporting_end_date >= d.day_date
), contracts_with_daily_usage as (
    -- Contracts joined with daily usage using the day dimension
    select
        c.*,
        u.total_bytes,
        u.total_rows,
        u.max_qpm,
        u.total_queries,
        sum(u.total_bytes) over (
            partition by c.contract_id
            order by c.reporting_date
            rows between unbounded preceding and current row
        ) as cumulative_bytes,
        sum(u.total_rows) over (
            partition by c.contract_id
            order by c.reporting_date
            rows between unbounded preceding and current row
        ) as cumulative_rows,
        max(u.max_qpm) over (
            partition by c.contract_id
            order by c.reporting_date
            rows between unbounded preceding and current row
        ) as cumulative_max_qpm
    from contracts_with_days c
    left join daily_contract_utilization u
        on c.contract_id = u.contract_id
        and c.reporting_date = u.reporting_date
), contracts_with_monthly_usage as (
    -- Rollup by month
    select
        contract_id,
        reporting_month,
        sum(total_bytes) as total_bytes,
        sum(total_rows) as total_rows,
        max(max_qpm) as max_qpm,
        sum(total_queries) as total_queries,
        max(cumulative_bytes) as cumulative_bytes,
        max(cumulative_rows) as cumulative_rows,
        max(cumulative_max_qpm) as cumulative_max_qpm
    from contracts_with_daily_usage
    group by 1, 2
)
select * from contracts_with_monthly_usage
