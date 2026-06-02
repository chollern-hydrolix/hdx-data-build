with per_deployment_month as (
    select
        reporting_month,
        contract_id,
        deployment_sfid,
        count(distinct reporting_date) as active_days,
        max(days_in_month) as days_in_month,
        sum(daily_mrr_prorated) as summed_daily_mrr,
        max(monthly_ending_mrr) as monthly_mrr
    from {{ ref('mart_cogs__daily_contract_margin') }}
    where monthly_ending_mrr is not null
    group by 1, 2, 3
)
select
    reporting_month,
    contract_id,
    deployment_sfid,
    active_days,
    days_in_month,
    summed_daily_mrr,
    monthly_mrr,
    monthly_mrr::numeric * active_days::numeric / days_in_month::numeric as expected_partial,
    summed_daily_mrr - monthly_mrr::numeric * active_days::numeric / days_in_month::numeric as diff
from per_deployment_month
where abs(
    summed_daily_mrr
    - monthly_mrr::numeric * active_days::numeric / days_in_month::numeric
) > 0.01