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
), joined as (
    select
        c.*,
        d.day_date as snapshot_date
    from {{ref('snapshot_crm__contract')}} c
    inner join days d
        on c.dbt_valid_from <= d.day_date
        and coalesce(c.dbt_valid_to, '2099-12-31') > d.day_date
), deduped as (
    select distinct on (contract_id, snapshot_date)
        *
    from joined
    order by contract_id, snapshot_date, dbt_valid_from desc
)
select
    {{ dbt_utils.star(from=ref('snapshot_crm__contract'), except=['dbt_scd_id', 'dbt_updated_at', 'dbt_valid_from', 'dbt_valid_to']) }},
    snapshot_date
from deduped
