{{ config(materialized="table") }}

with raw_actuals as (
    select * from {{ref('stg_planning_actuals')}}
), raw_plan_data as (
    select * from {{ref('stg_planning_plan_data')}}
), grouped_actuals as (
    select
        version_name,
        month,
        quarter,
        fiscal_year,
        dept_id,
        dept_title,
        dept_rollup,
        pl_rollup,
        sum(directional_amount) as amount
    from raw_actuals
    group by 1, 2, 3, 4, 5, 6, 7, 8
), grouped_plan_data as (
        select
        version_name,
        month,
        quarter,
        fiscal_year,
        dept_id,
        dept_title,
        dept_rollup,
        pl_rollup,
        sum(directional_amount) as amount
    from raw_plan_data
    group by 1, 2, 3, 4, 5, 6, 7, 8
), union_data as (
    select * from grouped_actuals
        union all
    select * from grouped_plan_data
)
select
    version_name,
    month,
    quarter,
    fiscal_year,
    coalesce(dept_id, '99999') as dept_id,
    coalesce(dept_title, 'No Department') as dept_title,
    coalesce(dept_rollup, 'No Department') as dept_rollup,
    pl_rollup,
    coalesce(amount, 0) as amount
from union_data
