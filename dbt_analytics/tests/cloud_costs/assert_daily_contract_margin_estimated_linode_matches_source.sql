-- Asserts that the sum of estimated_daily_linode_cost in mart_cogs__daily_contract_margin
-- is within 1% of the total from stg_linode_instance_billing, per month.
-- Includes UNALLOCATED rows so the totals on both sides are complete.
-- Covers Jan 2026 through the previous month.
with src as (
    select
        invoice_month,
        sum(total_amount) as src_total
    from {{ ref('stg_linode_instance_billing') }}
    where invoice_month
          between '2026-01-01' and date_trunc('month', current_date)::date - interval '1 month'
    group by 1
), in_mart as (
    select
        reporting_month,
        sum(estimated_daily_linode_cost) as mart_total
    from {{ ref('mart_cogs__daily_contract_margin') }}
    where reporting_month
          between '2026-01-01' and date_trunc('month', current_date)::date - interval '1 month'
    group by 1
)
select
    coalesce(s.invoice_month, m.reporting_month) as reporting_month,
    s.src_total,
    m.mart_total,
    abs(coalesce(s.src_total, 0) - coalesce(m.mart_total, 0))
        / nullif(abs(coalesce(s.src_total, 0)), 0) as pct_error
from src s
full outer join in_mart m on s.invoice_month = m.reporting_month
where abs(coalesce(s.src_total, 0) - coalesce(m.mart_total, 0))
    / nullif(abs(coalesce(s.src_total, 0)), 0) > 0.01