{{ config(materialized="table") }}

with mart_plan_financials as (
    select
        *
    from {{ref('mart_plan_financials')}}
), financials_by_month as (
    select
        month,
        quarter,
        fiscal_year,
        pl_rollup,
        dept_rollup,
        coalesce(sum(amount) filter (where version_name = 'Actuals'), 0) as amount_actual,
        coalesce(sum(amount) filter (where version_name = 'Budget'), 0) as amount_budget
    from mart_plan_financials
    group by 1, 2, 3, 4, 5
), financials_with_variance as (
    select
        *,
        amount_actual - amount_budget as bva_amount,
        coalesce((amount_actual - amount_budget) / nullif(amount_budget, 0), 0) as bva_percent
    from financials_by_month
), variance_final as (
    select
        month,
        quarter,
        fiscal_year,
        pl_rollup,
        dept_rollup,
        round(amount_actual::numeric, 2) as amount_actual,
        round(amount_budget::numeric, 2) as amount_budget,
        round(bva_amount::numeric, 2) as bva_amount,
        round(bva_percent::numeric, 4) as bva_percent
    from financials_with_variance
)
select * from variance_final
