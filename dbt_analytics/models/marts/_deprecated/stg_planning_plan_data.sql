{{ config(materialized="table") }}

select
    version_name,
    month,
    quarter,
    fiscal_year,
    department_id as dept_id,
    department_title as dept_title,
    fs_name as dept_rollup,
    pl_rollup,
    directional_amount
from google_sheets.budget_entry
