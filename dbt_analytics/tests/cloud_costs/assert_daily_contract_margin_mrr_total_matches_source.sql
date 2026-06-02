-- Asserts that the MRR represented in mart_cogs__daily_contract_margin is within 1% of
-- mart_mrr_contracts, aggregated to month level.
-- Because a contract can link to multiple deployments, monthly_ending_mrr is deduplicated
-- to one value per contract per month before summing, avoiding double-counting.
-- Covers Jan 2026 through the previous month.
with src as (
    select
        reporting_month,
        sum(ending_mrr_gross) as src_total
    from {{ ref('mart_mrr_contracts') }}
    where ending_mrr_gross is not null
      and reporting_month
          between '2026-01-01' and date_trunc('month', current_date)::date - interval '1 month'
    group by 1
), in_mart as (
    select
        reporting_month,
        sum(contract_mrr) as mart_total
    from (
        select
            reporting_month,
            contract_id,
            max(monthly_ending_mrr) as contract_mrr
        from {{ ref('mart_cogs__daily_contract_margin') }}
        where monthly_ending_mrr is not null
          and contract_id != 'UNALLOCATED'
          and reporting_month
              between '2026-01-01' and date_trunc('month', current_date)::date - interval '1 month'
        group by 1, 2
    ) deduped
    group by 1
)
select
    coalesce(s.reporting_month, m.reporting_month) as reporting_month,
    s.src_total,
    m.mart_total,
    abs(coalesce(s.src_total, 0) - coalesce(m.mart_total, 0))
        / nullif(abs(coalesce(s.src_total, 0)), 0) as pct_error
from src s
full outer join in_mart m on s.reporting_month = m.reporting_month
where abs(coalesce(s.src_total, 0) - coalesce(m.mart_total, 0))
    / nullif(abs(coalesce(s.src_total, 0)), 0) > 0.01