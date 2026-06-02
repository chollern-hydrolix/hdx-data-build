-- Asserts that the sum of invoiced_daily_linode_cost_prorated in mart_cogs__daily_contract_margin
-- is within 1% of total positive invoice items from fct_akm__invoice_item, per month.
-- Summing the prorated daily value across all rows (contract + UNALLOCATED) recovers the
-- full monthly invoiced total without double-counting across contracts or deployments.
-- Covers Jan 2026 through the previous month (invoices are published with a one-month lag).
with src as (
    select
        date_trunc('month', invoice_publish_month - interval '1 month')::date as invoice_month,
        sum(total) as src_total
    from {{ ref('fct_akm__invoice_item') }}
    where total >= 0
      and date_trunc('month', invoice_publish_month - interval '1 month')::date
          between '2026-01-01' and date_trunc('month', current_date)::date - interval '1 month'
    group by 1
), in_mart as (
    select
        reporting_month,
        sum(invoiced_daily_linode_cost_prorated) as mart_total
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