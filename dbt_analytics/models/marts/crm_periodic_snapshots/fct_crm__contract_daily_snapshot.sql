{{
    config(
        materialized='incremental',
        unique_key=['contract_id', 'snapshot_date'],
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['snapshot_date']},
            {'columns': ['contract_id']},
            {'columns': ['contract_id', 'snapshot_date'], 'unique': True}
        ]
    )
}}

with new_snapshot_dates as (
    select day_date as snapshot_date
    from {{ ref('dim_day') }}
    where day_date between '2026-01-01' and current_date
    {% if is_incremental() %}
      and day_date > (select coalesce(max(snapshot_date), '1900-01-01'::date) from {{ this }})
    {% endif %}
), versions as (
    select
        *,
        coalesce(dbt_valid_to, '2099-12-31'::date) as valid_to_filled
    from {{ ref('snapshot_crm__contract') }}
), joined as (
    select
        v.*,
        d.snapshot_date
    from versions v
    inner join new_snapshot_dates d
        on v.dbt_valid_from <= d.snapshot_date
       and v.valid_to_filled > d.snapshot_date
), deduped as (
    -- Multiple dbt snapshot versions can overlap the same snapshot_date; keep the latest.
    select distinct on (contract_id, snapshot_date)
        *
    from joined
    order by contract_id, snapshot_date, dbt_valid_from desc
)
select
    {{ dbt_utils.star(
        from=ref('snapshot_crm__contract'),
        except=['dbt_scd_id', 'dbt_updated_at', 'dbt_valid_from', 'dbt_valid_to']
    ) }},
    snapshot_date
from deduped
