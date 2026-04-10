{{ config(materialized="table") }}

with days as (
    SELECT * FROM {{ ref('dim_day') }}
    WHERE day_date BETWEEN '2025-01-01' AND CURRENT_DATE
), deployments as (
    select
        deployment_id,
        contract_id,
        cluster_label,
        coalesce(clean_cluster_hostname, '') as cluster_hostname,
        coalesce(cluster_project_name, '') as cluster_project_name,
        -- Rank to ensure multiple deployments aren't used for the same Contract
        row_number() over(
            partition by contract_id
            order by last_modified_date desc
        ) as _rank
    from {{ref('fct_deployment')}}
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
        c.contract_id,
        d.cluster_hostname,
        d.cluster_label,
        concat(d.cluster_hostname, '-', d.cluster_project_name) as merge_name
    from {{ref('fct_contract')}} c
    left join {{ref('dim_account')}} a on c.account_id = a.account_id
    left join {{ ref('fct_replacement_contract') }} repl_c on c.contract_id = repl_c.contract_id
    left join deployments d on c.contract_id = d.contract_id
    where c.status = 'Activated'
    and (d._rank = 1 or d._rank is null)
), contracts_with_days as (
    -- Join contracts with reporting_month to get a row for each combination of contract and reporting_month
    select
        c.*,
        d.day_date as reporting_date,
        d.month_date as reporting_month,
        extract(day from (date_trunc('month', d.month_date) + interval '1 month - 1 day')) as total_days_in_month
    from contracts c
    inner join days d
        on c.contract_start_date <= d.day_date
        and c.contract_end_date >= d.day_date
), contracts_with_days_in_month as (
    -- Add period start date, period end date, and contract days in month based on the start and end date of the contract and current reporting month
    select c.* from contracts_with_days c
), contracts_with_mrr_by_day as (
    -- Adjust MRR based on the number of days in the month
    select
        c.*,
        round((mrr_gross / total_days_in_month)::numeric, 2) as daily_mrr_gross,
        round((mrr_net / total_days_in_month)::numeric, 2) as daily_mrr_net
    from contracts_with_days_in_month c
), contracts_with_azure_usage as (
    -- Join contracts data with Azure usage by contract_id and reporting_month
    select
        c.*,
        round(coalesce(u.azure_cost, 0)::numeric, 2) as total_azure_cost
    from contracts_with_mrr_by_day c
    left join {{ref('stg_contract_azure_usage')}} u
        on c.contract_id = u.contract_id
        and c.reporting_date = u.azure_usage_date
), contracts_with_linode_usage as (
    select
        c.*,
        round(coalesce(l.total_amount, 0)::numeric, 2) as total_linode_cost
    from contracts_with_azure_usage c
    left join {{ref('stg_daily_contract_linode_usage')}} l
        on c.contract_id = l.contract_id
        and c.reporting_date = l.reporting_date
), contracts_with_margins as (
    select
        c.*,
        (total_azure_cost + total_linode_cost) as total_costs,
        (daily_mrr_gross - total_azure_cost - total_linode_cost) as gross_mrr_margin,
        (daily_mrr_net - total_azure_cost - total_linode_cost) as net_mrr_margin
    from contracts_with_linode_usage c
), contracts_final as (
    select
        c.*,
        round((gross_mrr_margin / nullif(daily_mrr_gross, 0)), 6) as gross_mrr_margin_pct,
        round((net_mrr_margin / nullif(daily_mrr_net, 0)), 6) as net_mrr_margin_pct
    from contracts_with_margins c
)
select * from contracts_final
