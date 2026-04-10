{{ config(materialized="table") }}

WITH months AS (
    SELECT * FROM {{ ref('dim_month') }}
    WHERE month_date BETWEEN '2022-01-01' AND '2030-12-31'
), contracts AS (
    select
        c.account_name,
        c.activated_effective_month,
        c.start_month::date,
        c.end_month::date,
        -- case
        --     -- If this contract replaced another mid-contract period
        --     when c.previous_contract_end_month > c.start_month then c.activated_effective_month::date
        --     -- Handle activated effective month following the contract duration
        --     when c.type_calculated = 'New Business' and c.activated_effective_month > c.end_month then c.start_month::date
        --     -- If New Business, Cancellation, or Expiration, use activated effective month
        --     when c.type_calculated in ('New Business', 'Cancellation', 'Expiration') then c.activated_effective_month::date
        --     else c.activated_effective_month::date
        -- end as reporting_effective_month,
        c.activated_effective_month::date as reporting_effective_month,
        case
            when c.replaced_by_new_contract is True and repl_c.replacement_end_month <= c.end_month then (repl_detail.activated_effective_month - interval '1 month')::date
            else c.end_month::date
            -- when c.replaced_by_new_contract is False then make_date(2027, 12, 1)::date
            -- else c.end_month::date
        end as reporting_end_month,
        c.mrr_gross,
        c.contract_id,
        c.contract_number,
        c.replaced_by_new_contract,
        c.is_bridge_renewal,
        c.type_calculated,
        c.type,
        c.channel,
        c.region,
        c.country,
        c.hydrolix_product,
        c.type_reporting,
        c.account_id,
        coalesce(repl_c.replacement_contract_id, 'None') as renewal_contract_id,
        coalesce(repl_c.replacement_contract_number, 'None') as renewal_contract_number,
        coalesce(repl_detail.mrr_gross, 0) as renewal_mrr_gross,
        coalesce(repl_detail.type_reporting, 'None') as renewal_type,
        c.type = 'Event' as is_event,
        c.replaced_by_draft_contract,
        c.churn_date_reporting_month,
        c.churn_confirmed,
        c.churn_confirmed_date,
        a.account_owner,
        c.sold_by,
        c.primary_industry,
        c.primary_sub_industry,
        c.commit_amount,
        coalesce(c.commit_type, 'N/A') as commit_type,
        next_opp.opportunity_id is not null as has_next_opportunity,
        coalesce(next_opp.owner_name, 'N/A') as next_opp_owner_name,
        coalesce(next_opp.stage_name, 'N/A') as next_opp_stage_name,
        next_opp.mrr_gross as next_opp_mrr_gross,
        next_opp.close_date as next_opp_close_date,
        next_opp.probability as next_opp_probability,
        next_opp.type_reporting as next_opp_type_reporting
    from {{ ref('stg_mrr_contracts') }} as c
    left join {{ ref('fct_replacement_contract') }} repl_c on c.contract_id = repl_c.contract_id
    left join {{ ref('stg_mrr_contracts') }} repl_detail on repl_c.replacement_contract_id = repl_detail.contract_id
    left join {{ ref('stg_next_opportunity_by_account') }} next_opp on c.account_id = next_opp.account_id
    left join {{ ref('dim_account') }} a on c.account_id = a.account_id
    where c.status = 'Activated'
    -- Filter out contracts replaced mid-term
    -- and not (
    --     c.replaced_by_new_contract and
    --     repl_detail.start_date < c.end_date
    -- )
), contracts_with_months AS (
    select
        c.*,
        m.month_date as reporting_month
    from contracts c
    inner join months m
        on c.reporting_effective_month <= m.month_date
        and c.reporting_end_month >= m.month_date
), contracts_with_flags as (
    select
        c.*,
        c.end_month = c.reporting_month and type_calculated not in ('Cancellation', 'Expiration') as is_up_for_renewal,
        c.end_month = c.reporting_month and type_calculated not in ('Cancellation', 'Expiration') and c.replaced_by_new_contract is True as is_renewed,
        case
            when c.renewal_type in ('Cancellation', 'Expiration') or c.churn_confirmed then 'churn'
            when c.renewal_type = 'None' then 'outstanding'
            when c.mrr_gross = c.renewal_mrr_gross then 'flat'
            when c.is_bridge_renewal then 'bridge'
            when c.mrr_gross > c.renewal_mrr_gross then 'downgrade'
            when c.mrr_gross < c.renewal_mrr_gross then 'upgrade'
            else 'N/A'
        end as renewal_category
    from contracts_with_months c
), contracts_with_values as (
    select
        c.*,
        case when c.is_up_for_renewal then c.mrr_gross else 0 end as total_mrr_expiring,
        case when c.is_up_for_renewal and c.renewal_category = 'flat' then c.renewal_mrr_gross else 0 end as flat_renewal_mrr,
        case when c.is_up_for_renewal and c.renewal_category = 'bridge' then c.renewal_mrr_gross else 0 end as bridge_renewal_mrr,
        case when c.is_up_for_renewal and c.renewal_category = 'downgrade' then c.renewal_mrr_gross else 0 end as downgrade_renewal_mrr,
        case when c.is_up_for_renewal and c.renewal_category = 'upgrade' then c.renewal_mrr_gross else 0 end as upgrade_renewal_mrr,
        case when c.is_up_for_renewal and c.renewal_category = 'churn' then -c.mrr_gross else 0 end as churn_mrr,
        case when c.is_up_for_renewal then (
            -- Total MRR Expiring
            case when c.is_up_for_renewal then c.mrr_gross else 0 end -
            -- Less: Initial MRR of Flat, Upgrade, Downgrade, and Bridge Renewals
            case when c.renewal_category in ('flat', 'bridge', 'upgrade', 'downgrade') then c.mrr_gross else 0 end -
            -- Less: Churn MRR
            case when c.renewal_category = 'churn' then c.mrr_gross else 0 end
        ) else 0 end as total_mrr_outstanding
    from contracts_with_flags c
), contracts_with_counts as (
    select
        c.*,
        case when c.is_up_for_renewal and c.renewal_category = 'downgrade' and c.renewal_contract_id != 'None' then round((c.renewal_mrr_gross - c.mrr_gross)::numeric, 2) else 0 end as downgraded_mrr,
        case when c.is_up_for_renewal and c.renewal_category = 'upgrade' and c.renewal_contract_id != 'None' then round((c.renewal_mrr_gross - c.mrr_gross)::numeric, 2) else 0 end as upgraded_mrr,
        case when c.is_up_for_renewal then 1 else 0 end as total_contracts_expiring,
        case when c.is_up_for_renewal and c.renewal_category = 'flat' then 1 else 0 end as flat_renewal_contracts,
        case when c.is_up_for_renewal and c.renewal_category = 'bridge' then 1 else 0 end as bridge_renewal_contracts,
        case when c.is_up_for_renewal and c.renewal_category = 'downgrade' then 1 else 0 end as downgrade_renewal_contracts,
        case when c.is_up_for_renewal and c.renewal_category = 'upgrade' then 1 else 0 end as upgrade_renewal_contracts,
        case when c.is_up_for_renewal and c.renewal_category = 'churn' then 1 else 0 end as churn_contracts,
        case when c.is_up_for_renewal then (
            -- Total MRR Expiring
            case when c.is_up_for_renewal then 1 else 0 end -
            -- Less: Count of Flat, Upgrade, Downgrade, and Bridge Renewals
            case when c.renewal_category in ('flat', 'bridge', 'upgrade', 'downgrade') then 1 else 0 end -
            -- Less: Churned Contracts
            case when c.renewal_category = 'churn' then 1 else 0 end
        ) else 0 end as total_renewals_outstanding
    from contracts_with_values c
), contracts_with_usage as (
    select
        c.*,
        u.total_bytes,
        u.total_rows,
        case
            when c.commit_type != 'Billion records per month' then u.total_bytes
            else u.total_rows
        end as total_usage_normalized,
        u.cumulative_bytes,
        u.cumulative_rows,
        case
            when c.commit_type != 'Billion records per month' then u.cumulative_bytes
            else u.cumulative_rows
        end as cumulative_usage_normalized,
        u.max_qpm,
        coalesce(u.total_queries, 0) as total_queries,
        u.cumulative_max_qpm
    from contracts_with_counts c
    left join {{ref('stg_usage__contract_monthly')}} u
        on c.contract_id = u.contract_id
        and c.reporting_month = u.reporting_month
), contracts_with_filter_helper as (
    select
        c.*,
        (total_mrr_expiring = 0 and flat_renewal_mrr = 0 and bridge_renewal_mrr = 0 and downgrade_renewal_mrr = 0 and upgrade_renewal_mrr = 0 and total_mrr_outstanding = 0) as should_remove
    from contracts_with_usage c
)
select * from contracts_with_filter_helper
where should_remove is False
