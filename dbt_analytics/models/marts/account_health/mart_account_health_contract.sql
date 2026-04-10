{{ config(materialized="table") }}

with contracts as (
    select
        c.status,
        c.contract_number,
        c.contract_id,
        c.replaced_by_new_contract,
        c.channel,
        c.region,
        c.country,
        c.type_calculated,
        c.mrr_gross,
        c.mrr_net,
        a.account_name,
        c.hydrolix_product,
        c.commit_amount,
        c.commit_type,
        c.commit_normalized,
        c.contract_start_date,
        c.contract_end_date,
        c.contract_start_month,
        c.contract_end_month,
        contract_short_id
    from {{ref('fct_contract')}} c
    left join {{ref('dim_account')}} a on c.account_id = a.account_id
), daily_usage as (
    select
        contract_id,
        usage_last_7_days,
        usage_days_last_7_days,
        avg_daily_gbs_last_7_days,
        max_qpm_last_7_days
    from {{ref('fct_contract_daily_usage')}}
    where date = (select max(date) from {{ref('fct_contract_daily_usage')}})
), contracts_with_usage as (
    select
        c.*,
        coalesce(u.usage_last_7_days, 0) as usage_last_7_days,
        coalesce(u.usage_days_last_7_days, 0) as usage_days_last_7_days,
        coalesce(u.avg_daily_gbs_last_7_days, 0) as avg_daily_gbs_last_7_days,
        coalesce(u.max_qpm_last_7_days, 0) as max_qpm_last_7_days
    from contracts c
    left join daily_usage u on c.contract_short_id = u.contract_id
), contracts_with_metrics as (
    select
        c.*,
        coalesce(((c.usage_last_7_days / nullif(c.usage_days_last_7_days, 0)) * 365 / 12 / nullif(c.commit_normalized, 0)), 0) as pct_of_commit_last_7_days
    from contracts_with_usage c
), contracts_with_groups as (
    select
        c.*,
        case
            when c.pct_of_commit_last_7_days = 0 then '1) 0%'
            when c.pct_of_commit_last_7_days < 0.25 then '2) < 25%'
            when c.pct_of_commit_last_7_days < 0.5 then '3) < 50%'
            when c.pct_of_commit_last_7_days < 0.75 then '4) < 75%'
            when c.pct_of_commit_last_7_days <= 1 then '5) <= 100%'
            else '6) > 100%'
        end as pct_of_commit_group,
        case
            when c.max_qpm_last_7_days is null or c.max_qpm_last_7_days = 0 then '1) 0'
            when c.max_qpm_last_7_days < 25 then '2) < 25'
            when c.max_qpm_last_7_days < 50 then '3) < 50'
            when c.max_qpm_last_7_days < 75 then '4) < 75'
            when c.max_qpm_last_7_days <= 100 then '5) <= 100'
            else '6) > 100'
        end as max_qpm_group
    from contracts_with_metrics c
)
select * from contracts_with_groups
