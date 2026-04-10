{{
    config(
        materialized="table",
        indexes=[
            {'columns': ['snapshot_date']}
        ]
    )
}}

with days as (
    select
        day_date
    from {{ ref('dim_day') }}
    where day_date between '2026-01-01' and current_date
)
select
    {{ dbt_utils.star(from=ref('snapshot_crm__deployment'), except=['dbt_scd_id', 'dbt_updated_at', 'dbt_valid_from', 'dbt_valid_to']) }},
    d.day_date as snapshot_date
from {{ref('snapshot_crm__deployment')}} c
inner join days d
    on c.dbt_valid_from <= d.day_date
    and coalesce(c.dbt_valid_to, '2099-12-31') > d.day_date
