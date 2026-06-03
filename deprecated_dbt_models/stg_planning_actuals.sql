{{ config(materialized="table") }}

select
    'Actuals' as version_name,
    month,
    quarter,
    fiscal_year,
    department_id as dept_id,
    department_title as dept_title,
    fs_name as dept_rollup,
    pl_rollup,
    directional_amount
from sage.vm_gl_entry
