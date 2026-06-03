{{ config(materialized="table") }}

with months as (
    SELECT * FROM {{ ref('dim_month') }}
    WHERE month_date BETWEEN '2022-01-01' AND CURRENT_DATE
), contracts as (
    -- Get Activated Contracts
    select
        a.account_name,
        c.contract_number,
        case
            when c.activated_effective_month is not null then c.activated_effective_date::date
            else c.contract_start_date::date
        end as contract_start_date,
        case
            when repl_c.replacement_activated_effective_month is not null then (repl_c.replacement_activated_effective_date - interval '1 day')::date
            else c.contract_end_date::date
        end as contract_end_date,
        case
            when c.activated_effective_month is not null then c.activated_effective_month::date
            else c.contract_start_month::date
        end as contract_start_month,
        case
            -- If replacement contract exists: run this contract through the month preceding the replacement contract activated effective month
            -- when repl_c.replacement_activated_effective_month is not null then (repl_c.replacement_activated_effective_month - interval '1 month')::date
            when repl_c.replacement_activated_effective_month is not null then date_trunc('month', repl_c.replacement_activated_effective_date - interval '1 day')::date
            else c.contract_end_month::date
        end as contract_end_month,
        c.status,
        c.type,
        c.type_calculated,
        c.channel,
        c.region,
        c.country,
        c.hydrolix_product,
        c.mrr_gross,
        c.mrr_net,
        a.account_id,
        c.contract_id
    from {{ref('fct_contract')}} c
    left join {{ref('dim_account')}} a on c.account_id = a.account_id
    left join {{ ref('fct_replacement_contract') }} repl_c on c.contract_id = repl_c.contract_id
    where c.status = 'Activated'
), contracts_with_months as (
    -- Join contracts with reporting_month to get a row for each combination of contract and reporting_month
    select
        c.*,
        m.month_date as reporting_month,
        extract(day from (date_trunc('month', m.month_date) + interval '1 month - 1 day')) as total_days_in_month
    from contracts c
    inner join months m
        on c.contract_start_month <= m.month_date
        and c.contract_end_month >= m.month_date
), contracts_with_days_in_month as (
    -- Add period start date, period end date, and contract days in month based on the start and end date of the contract and current reporting month
    select
        c.*,
        case
            when c.reporting_month = c.contract_start_month then c.contract_start_date::date
            when c.reporting_month = c.contract_end_month then date_trunc('month', c.contract_end_date)::date
            else c.reporting_month::date
        end as period_start_date,
        case
            when c.reporting_month = c.contract_start_month then (date_trunc('month', c.contract_start_date) + interval '1 month - 1 day')::date
            when c.reporting_month = date_trunc('month', CURRENT_DATE)::date then (CURRENT_DATE - interval '1 day')::date
            when c.reporting_month = c.contract_end_month then c.contract_end_date::date
            else (date_trunc('month', c.reporting_month) + interval '1 month - 1 day')::date
        end as period_end_date,
        case
            when c.reporting_month = c.contract_start_month then date_trunc('month', c.reporting_month + interval '1 month')::date - c.contract_start_date::date
            when c.reporting_month = date_trunc('month', CURRENT_DATE)::date then (extract(day from CURRENT_DATE))  -- When reporting_month = current_month then use current day as contract_days_in_month
            when c.reporting_month = c.contract_end_month then (c.contract_end_date::date - c.reporting_month::date) + 1
            else total_days_in_month
        end as contract_days_in_month
    from contracts_with_months c
), contracts_with_mrr_by_day as (
    -- Adjust MRR based on the number of contracted days in the month
    select
        c.*,
        round((mrr_gross / total_days_in_month * contract_days_in_month)::numeric, 2) as adjusted_mrr_gross,
        round((mrr_net / total_days_in_month * contract_days_in_month)::numeric, 2) as adjusted_mrr_net
    from contracts_with_days_in_month c
), contracts_with_azure_usage as (
    -- Align Azure daily usage to the correct contract and reporting month
    select
        c.contract_id,
        c.reporting_month,
        sum(azure_cost) as total_azure_cost
    from contracts_with_mrr_by_day c
    left join {{ref('stg_contract_azure_usage')}} d
        on c.contract_id = d.contract_id
        and c.period_start_date <= d.azure_usage_date
        and c.period_end_date >= d.azure_usage_date
    group by 1, 2
), contracts_with_agg_azure_usage as (
    -- Join contracts data with Azure usage by contract_id and reporting_month
    select
        c.*,
        round(coalesce(u.total_azure_cost, 0)::numeric, 2) as total_azure_cost
    from contracts_with_mrr_by_day c
    left join contracts_with_azure_usage u
        on c.contract_id = u.contract_id
        and c.reporting_month = u.reporting_month
), contracts_with_linode_usage as (
    select
        c.*,
        round(
            (c.contract_days_in_month / c.total_days_in_month) * coalesce(l.total_amount, 0)::numeric,
            2
        ) as total_linode_cost
    from contracts_with_agg_azure_usage c
    left join {{ref('stg_contract_linode_usage')}} l
        on c.contract_id = l.contract_id
        and c.reporting_month = l.invoice_month
), contracts_with_margins as (
    select
        c.*,
        -(total_azure_cost + total_linode_cost) as total_costs,
        (adjusted_mrr_gross - total_azure_cost - total_linode_cost) as gross_mrr_margin,
        (adjusted_mrr_net - total_azure_cost - total_linode_cost) as net_mrr_margin
    from contracts_with_linode_usage c
    -- Filter for 2025 and beyond because we were not tracking Linode or Azure costs prior to this
    where reporting_month >= '2025-01-01'
), contracts_final as (
    select
        c.*,
        round((gross_mrr_margin / nullif(adjusted_mrr_gross, 0)), 6) as gross_mrr_margin_pct,
        round((net_mrr_margin / nullif(adjusted_mrr_net, 0)), 6) as net_mrr_margin_pct
    from contracts_with_margins c
)
select * from contracts_final
