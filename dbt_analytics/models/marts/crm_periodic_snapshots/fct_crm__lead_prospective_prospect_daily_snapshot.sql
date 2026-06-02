{{
    config(
        materialized='incremental',
        unique_key=['lead_id', 'snapshot_date'],
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['snapshot_date']},
            {'columns': ['lead_id']},
            {'columns': ['lead_id', 'snapshot_date'], 'unique': True}
        ]
    )
}}

with new_snapshot_dates as (
    select day_date as snapshot_date
    from {{ ref('dim_day') }}
    where day_date between '2026-03-01' and current_date
      and day_of_week between 1 and 5
    {% if is_incremental() %}
      and day_date > (select coalesce(max(snapshot_date), '1900-01-01'::date) from {{ this }})
    {% endif %}
), versions as (
    select
        *,
        coalesce(dbt_valid_to, '2099-12-31'::date) as valid_to_filled
    from {{ ref('snapshot_crm__lead') }}
    where lead_record_type = 'Prospective Prospect'
)
select
    {{ dbt_utils.star(
        from=ref('snapshot_crm__lead'),
        except=['dbt_scd_id', 'dbt_updated_at', 'dbt_valid_from', 'dbt_valid_to', 'bdr_notes'],
        relation_alias='v'
    ) }},
    d.snapshot_date
from versions v
inner join new_snapshot_dates d
    on v.dbt_valid_from <= d.snapshot_date
   and v.valid_to_filled > d.snapshot_date
