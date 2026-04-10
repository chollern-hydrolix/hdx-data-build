-- Mirror Brad's D-Usage-Contracts

{{ config(materialized="view") }}

with rolling_qpm as (
    select
        *
    from argus.daily_rolling_qpm_with_contract
), rolling_qpm_with_metrics as (
    select
        q.*,
        q.bytes_last_7_days / (1024 ^ 3) as avg_daily_gbs_last_7_days,
        coalesce(q.hdx_pricing_last_7_days / nullif(q.usage_last_7_days, 0), 0) as avg_daily_hdx_pricing
    from rolling_qpm q
)
select * from rolling_qpm_with_metrics
