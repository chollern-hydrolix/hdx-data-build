-- Asserts that the sum of azure_cost in fct_cogs__azure_bucket_cost is within 1%
-- of the total pre_tax_cost in azure.monthly_cost_fact, per billing month.
-- Covers Jan 2026 through the previous month (azure invoices publish with a one-month lag).
with src as (
    select
        billing_month,
        sum(pre_tax_cost) as src_total
    from {{ source('azure', 'monthly_cost_fact') }}
    where billing_month
          between '2026-01-01' and date_trunc('month', current_date)::date - interval '1 month'
    group by 1
), in_fact as (
    select
        invoice_month,
        sum(azure_cost) as fact_total
    from {{ ref('fct_cogs__azure_bucket_cost') }}
    where invoice_month
          between '2026-01-01' and date_trunc('month', current_date)::date - interval '1 month'
    group by 1
)
select
    coalesce(s.billing_month, f.invoice_month) as billing_month,
    s.src_total,
    f.fact_total,
    abs(coalesce(s.src_total, 0) - coalesce(f.fact_total, 0))
        / nullif(abs(coalesce(s.src_total, 0)), 0) as pct_error
from src s
full outer join in_fact f on s.billing_month = f.invoice_month
where abs(coalesce(s.src_total, 0) - coalesce(f.fact_total, 0))
    / nullif(abs(coalesce(s.src_total, 0)), 0) > 0.01