with src as (
    select
        reporting_month,
        contract_id,
        sum(ending_mrr_gross) as src_mrr
    from {{ ref('mart_mrr_contracts') }}
    where ending_mrr_gross is not null
      and reporting_month between '2025-01-01' and current_date
    group by 1, 2
), in_mart as (
    select
        reporting_month,
        contract_id,
        max(monthly_ending_mrr) as mart_mrr
    from {{ ref('mart_cogs__daily_contract_margin') }}
    where monthly_ending_mrr is not null
      and contract_id != 'UNALLOCATED'
    group by 1, 2
)
select
    coalesce(s.reporting_month, m.reporting_month) as reporting_month,
    coalesce(s.contract_id, m.contract_id)        as contract_id,
    s.src_mrr,
    m.mart_mrr,
    coalesce(s.src_mrr, 0) - coalesce(m.mart_mrr, 0) as diff
from src s
full outer join in_mart m
    on s.reporting_month = m.reporting_month
   and s.contract_id    = m.contract_id
where abs(coalesce(s.src_mrr, 0) - coalesce(m.mart_mrr, 0)) > 0.01