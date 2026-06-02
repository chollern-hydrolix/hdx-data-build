{{
    config(
        materialized='incremental',
        unique_key='daily_billing_id',
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['linode_id']},
            {'columns': ['reporting_date']},
            {'columns': ['invoice_month']}
        ]
    )
}}

with monthly as (
    select *
    from {{ ref('stg_linode_instance_billing') }}
    {% if is_incremental() %}
    where invoice_month >= (select date_trunc('month', max(reporting_date))::date - interval '1 month' from {{ this }})
    {% endif %}
), daily_expanded as (
    select
        m.*,
        gs::date as reporting_date,
        count(*) over (partition by m.linode_id, m.invoice_month) as active_days_in_month
    from monthly m
    cross join lateral generate_series(
        m.billing_interval_start_timestamp::date,
        (m.billing_interval_end_timestamp - interval '1 second')::date,
        interval '1 day'
    ) gs
)
select
    {{ dbt_utils.generate_surrogate_key(['linode_id', 'reporting_date']) }} as daily_billing_id,
    cluster_id,
    cluster_label,
    cluster_region,
    linode_id,
    linode_label,
    linode_type_label,
    cloud_account,
    invoice_month,
    reporting_date,
    active_days_in_month,
    total_amount,
    hdx_amount,
    total_amount::numeric / active_days_in_month as daily_total_amount,
    hdx_amount::numeric / active_days_in_month as daily_hdx_amount
from daily_expanded