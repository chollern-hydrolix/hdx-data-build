-- Asserts that total Azure cost in mart_cogs__monthly_cost_report is within 1%
-- of mart_cogs__daily_contract_margin at the monthly level.
--
-- Both models source from fct_cogs__azure_bucket_cost, so variance should be minimal.
-- The daily margin model allocates costs to deployments and captures the remainder in
-- UNALLOCATED rows; summing all rows recovers the full monthly Azure total.
-- Covers Jan 2026 through the previous month.
with cost_report as (
    select
        invoice_month,
        sum(azure_cost) as src_total
    from {{ ref('mart_cogs__monthly_cost_report') }}
    where cloud_provider = 'Azure'
      and invoice_month
          between '2026-01-01' and date_trunc('month', current_date)::date - interval '1 month'
    group by 1
), daily_margin as (
    select
        reporting_month,
        sum(daily_azure_bucket_cost_prorated) as mart_total
    from {{ ref('mart_cogs__daily_contract_margin') }}
    where reporting_month
          between '2026-01-01' and date_trunc('month', current_date)::date - interval '1 month'
    group by 1
)
select
    coalesce(c.invoice_month, m.reporting_month) as reporting_month,
    c.src_total,
    m.mart_total,
    abs(coalesce(c.src_total, 0) - coalesce(m.mart_total, 0))
        / nullif(abs(coalesce(c.src_total, 0)), 0) as pct_error
from cost_report c
full outer join daily_margin m on c.invoice_month = m.reporting_month
where abs(coalesce(c.src_total, 0) - coalesce(m.mart_total, 0))
    / nullif(abs(coalesce(c.src_total, 0)), 0) > 0.01