{{config(materialized="table")}}

/* This must mirror mart_mrr_contract: replacement contract START DATES used instead of ACTIVATED EFFECTIVE DATE for accounting purposes */

WITH months AS (
    SELECT * FROM {{ ref('dim_month') }}
    WHERE month_date BETWEEN '2021-01-01' AND '2030-12-31'
), contracts AS (
    select
        c.account_name,
        c.activated_effective_month,
        c.start_date as contract_start_date,
        c.end_date as contract_end_date,
        c.contract_term as contract_term_months,
        c.start_month,
        -- If this contract was replaced by another mid-contract period: use the month prior to the replacement start month to end this contract
        case
            -- If replacement contract exists: run this contract through the month preceding the replacement contract activated effective month
            when repl_c.replacement_start_month is not null then (repl_c.replacement_start_month - interval '1 month')
            -- If type is Cancellation or Expiration: make sure end_month is on or after activated_effective_month
            when c.type_calculated in ('Cancellation', 'Expiration') and c.activated_effective_month > c.end_month then c.activated_effective_month
            -- Otherwise, use end_month
            else c.end_month
        end as end_month,
        c.previous_contract_start_month,
        c.previous_contract_end_month,
        coalesce(prev_c.type_calculated, 'None') as previous_contract_type_calculated,
        case
            -- If this contract replaced another mid-contract period
            when c.previous_contract_end_month > c.start_month then c.start_month
            -- Handle activated effective month following the contract duration
            when c.type_calculated = 'New Business' and c.start_month > c.end_month then c.start_month
            -- If New Business, Cancellation, or Expiration, use activated effective month
            when c.type_calculated in ('New Business', 'Cancellation', 'Expiration') then c.start_month
            -- Otherwise, use activated_effective_month
            else c.start_month
        end as reporting_effective_month,
        c.type,
        c.channel,
        c.region,
        c.country,
        c.hydrolix_product,
        c.type_calculated,
        c.type_reporting,
        case
            when c.type_calculated in ('Cancellation', 'Expiration') then c.activated_effective_month
            else make_date(2099, 12, 1)
        end as churn_month,
        c.mrr_net as mrr_net,
        c.mrr_gross as mrr_gross,
        c.gmrr_net as gmrr_net,
        c.gmrr_gross as gmrr_gross,
        c.lmrr_net as lmrr_net,
        c.lmrr_gross as lmrr_gross,
        case when c.churn_mrr_net_2025 = 0 then c.churn_mrr_net else c.churn_mrr_net_2025 end as churned_mrr_net,
        case when c.churn_mrr_gross_2025 = 0 then c.churn_mrr_net / 0.8 else c.churn_mrr_gross_2025 end as churned_mrr_gross,
        c.account_id,
        c.contract_number,
        c.contract_id,
        c.status,
        c.commit_amount,
        c.commit_type,
        c.fx_rate,
        c.fx_impact_mrr,
        c.customer_count_impact,
        c.activated_effective_date,
        repl_c.replacement_start_month,
        repl_c.replacement_end_month,
        o.lead_source as lead_source,
        o.lead_source_details as lead_source_details,
        o.lead_source_details_other as lead_source_details_other,
        c.sold_by,
        c.contract_active,
        c.akamai_sales_rep,
        c.primary_industry,
        c.primary_sub_industry,
        o.opportunity_id as opportunity_id
    from {{ ref('stg_crm__mrr_contract') }} c
    left join {{ ref('stg_crm__mrr_contract') }} as prev_c on c.previous_contract_id = prev_c.contract_id
    left join {{ ref('fct_crm__replacement_contract') }} repl_c on c.contract_id = repl_c.contract_id
    left join {{ ref('fct_crm__opportunity') }} o on c.opportunity_id = o.opportunity_id
), contracts_with_months AS (
    select
        c.*,
        m.month_date as reporting_month,
        date_trunc('quarter', m.month_date)::date as reporting_quarter,
        concat('Q', extract(quarter from m.month_date)::int, ' ', to_char(m.month_date, 'YYYY')) as reporting_quarter_label,
        coalesce((c.previous_contract_end_month >= m.month_date), false) as contract_overlaps_previous
    from contracts c
    inner join months m
        on c.reporting_effective_month <= m.month_date
        and c.end_month >= m.month_date
), stg_account_contract_rank as (
    select
        account_id,
        contract_id,
        reporting_month,
        contract_start_date,
        contract_end_date,
        customer_count_impact,
        activated_effective_date,
        case
            when type_reporting = 'New Business' then 1
            else 2
        end as type_reporting_rank
    from contracts_with_months
    where type != 'Event'
), account_contract_rank as (
    -- Get the rank of a Contract for a given Account and Reporting Month
    select
        account_id,
        contract_id,
        reporting_month,
        contract_start_date,
        contract_end_date,
        customer_count_impact,
        rank() over (
            partition by account_id, reporting_month
            order by activated_effective_date, type_reporting_rank, contract_start_date, contract_end_date, contract_id
        ) as contract_rank
    from stg_account_contract_rank
), customer_count_impact as (
    -- Get the customer start and end date based on initial and cancellation / expiration contract (if applicable)
    select
        account_id,
        min(case when customer_count_impact = 1 then reporting_month else make_date(2099, 12, 1) end) as start_month,
        min(case when customer_count_impact = -1 then reporting_month else make_date(2099, 12, 1) end) as end_month
    from contracts_with_months
    where customer_count_impact in (-1, 1)
    group by 1
), contracts_with_customer_counts as (
    select
        c.*,
        -- Customer counts
        -- Beginning Customers
        case
            when c_rank.contract_rank = 1 and c.reporting_month > cust_cnt.start_month and cust_cnt.end_month >= c.reporting_month and status = 'Activated' then 1
            else 0
        end as beginning_customers,
        
        -- New Customers
        case when c_rank.contract_rank = 1 and c.reporting_month = cust_cnt.start_month and status = 'Activated' then 1 else 0 end as new_customers,
        
        -- Churned Customers
        case when c_rank.contract_rank = 1 and c.reporting_month = cust_cnt.end_month and status = 'Activated' then -1 else 0 end as churned_customers,
        
        -- Ending Customers
        case
            when c_rank.contract_rank = 1 and c.reporting_month >= cust_cnt.start_month and cust_cnt.end_month > c.reporting_month and status = 'Activated' then 1
            else 0
        end as ending_customers
    from contracts_with_months c
    left join account_contract_rank c_rank
        on c.contract_id = c_rank.contract_id
        and c.reporting_month = c_rank.reporting_month
    left join customer_count_impact cust_cnt on c.account_id = cust_cnt.account_id
), contracts_with_mrr_stg as (
    select
        c.*,
        -- MRR net waterfall
        -- Beginning MRR Net
        case
            when type_reporting = 'New Business' and c.reporting_month > c.reporting_effective_month and c.churn_month >= c.reporting_month then c.gmrr_net + c.lmrr_net
            when type_reporting = 'Expansion' and c.reporting_month > c.reporting_effective_month and c.churn_month >= c.reporting_month then c.gmrr_net + c.lmrr_net
            when type_calculated = 'Renewal' then
                case
                    when c.previous_contract_type_calculated != 'Event' and c.reporting_month = c.reporting_effective_month and c.churn_month >= c.reporting_month then c.mrr_net - (c.fx_impact_mrr * 0.8)
                    when c.previous_contract_type_calculated != 'Event' and c.reporting_month > c.reporting_effective_month and c.churn_month >= c.reporting_month then c.mrr_net
                    when c.previous_contract_type_calculated = 'Event' and c.reporting_month > c.reporting_effective_month and c.churn_month >= c.reporting_month then c.mrr_net
                    else 0
                end
            when type_calculated in ('Upgrade', 'Downgrade') then
                case
                    when c.previous_contract_type_calculated = 'Event' and c.reporting_month = c.reporting_effective_month and c.churn_month >= c.reporting_month then 0
                    else
                        case
                            when c.reporting_month = c.reporting_effective_month and c.churn_month >= c.reporting_month then c.mrr_net - c.gmrr_net - c.lmrr_net - (c.fx_impact_mrr * 0.8)
                            when c.reporting_month > c.reporting_effective_month and c.churn_month >= c.reporting_month then c.mrr_net
                            else 0
                        end
                end
            when type_calculated = 'Expiration' and c.reporting_month = c.churn_month then c.mrr_net
            when type_calculated = 'Cancellation' and c.reporting_month = c.reporting_effective_month and c.reporting_month >= c.churn_month then -c.lmrr_net
            else 0
        end as beginning_mrr_net,
        
        -- New MRR Net
        -- c.new_customers * mrr_net as new_mrr_net,
        case
            when c.reporting_month = reporting_effective_month and type_reporting = 'New Business' then c.mrr_net
            else 0
        end as new_mrr_net,
        
        -- Expansion MRR Net
        case when c.reporting_month = c.reporting_effective_month and type_reporting != 'New Business' then c.gmrr_net else 0 end as expansion_mrr_net,
        
        -- Downgrade MRR Net
        case
            when c.reporting_month = c.reporting_effective_month and type_calculated = 'Downgrade' and previous_contract_type_calculated = 'Event' then 0
            when c.reporting_month = c.reporting_effective_month and type_calculated not in ('Cancellation', 'Expiration') then c.lmrr_net
            else 0
        end as downgrade_mrr_net,
        
        -- Churn MRR Net
        -- -(churned_customers * c.churned_mrr_net) as churn_mrr_net,
        case
            when c.reporting_month = c.churn_month then c.churned_mrr_net
            else 0
        end as churn_mrr_net,

        -- FX Impact MRR Net
        case when c.reporting_month = c.reporting_effective_month then (c.fx_impact_mrr * 0.8) else 0 end as fx_impact_mrr_net,
        
        -- MRR gross waterfall
        -- Beginning MRR Gross
        case
            when type_reporting = 'New Business' and c.reporting_month > c.reporting_effective_month and c.churn_month >= c.reporting_month then c.gmrr_gross + c.lmrr_gross
            when type_reporting = 'Expansion' and c.reporting_month > c.reporting_effective_month and c.churn_month >= c.reporting_month then c.gmrr_gross + c.lmrr_gross
            when type_calculated = 'Renewal' then
                case
                    when c.previous_contract_type_calculated != 'Event' and c.reporting_month = c.reporting_effective_month and c.churn_month >= c.reporting_month then c.mrr_gross - c.fx_impact_mrr
                    when c.previous_contract_type_calculated != 'Event' and c.reporting_month > c.reporting_effective_month and c.churn_month >= c.reporting_month then c.mrr_gross
                    when c.previous_contract_type_calculated = 'Event' and c.reporting_month > c.reporting_effective_month and c.churn_month >= c.reporting_month then c.mrr_gross
                    else 0
                end
            when type_calculated in ('Upgrade', 'Downgrade') then
                case
                    when c.previous_contract_type_calculated = 'Event' and c.reporting_month = c.reporting_effective_month and c.churn_month >= c.reporting_month then 0
                    else
                        case
                            when c.reporting_month = c.reporting_effective_month and c.churn_month >= c.reporting_month then c.mrr_gross - c.gmrr_gross - c.lmrr_gross - c.fx_impact_mrr
                            when c.reporting_month > c.reporting_effective_month and c.churn_month >= c.reporting_month then c.mrr_gross
                            else 0
                        end
                end
            when type_calculated = 'Expiration' and c.reporting_month = c.churn_month then c.mrr_gross
            when type_calculated = 'Cancellation' and c.reporting_month = c.reporting_effective_month and c.reporting_month >= c.churn_month then -c.lmrr_gross
            else 0
        end as beginning_mrr_gross,
        
        -- New MRR Gross
        -- c.new_customers * mrr_gross as new_mrr_gross,
        case
            when c.reporting_month = reporting_effective_month and type_reporting = 'New Business' then c.mrr_gross
            else 0
        end as new_mrr_gross,
        
        -- Expansion MRR Gross
        case when c.reporting_month = c.reporting_effective_month and type_reporting != 'New Business' then c.gmrr_gross else 0 end as expansion_mrr_gross,
        
        -- Downgrade MRR Gross
        case
            when c.reporting_month = c.reporting_effective_month and type_calculated = 'Downgrade' and previous_contract_type_calculated = 'Event' then 0
            when c.reporting_month = c.reporting_effective_month and type_calculated not in ('Cancellation', 'Expiration') then c.lmrr_gross
            else 0
        end as downgrade_mrr_gross,
        
        -- Churn MRR Gross
        -- -(churned_customers * c.churned_mrr_gross) as churn_mrr_gross,
        case
            when c.reporting_month = c.churn_month then c.churned_mrr_gross
            else 0
        end as churn_mrr_gross,

        -- FX Impact MRR Gross
        case when c.reporting_month = c.reporting_effective_month then c.fx_impact_mrr else 0 end as fx_impact_mrr_gross
    from contracts_with_customer_counts c
), contracts_with_mrr_totals as (
    select
        c.*,
        -- Gross / Net MRR Totals
        (c.beginning_mrr_gross + c.new_mrr_gross + c.expansion_mrr_gross + c.downgrade_mrr_gross + c.churn_mrr_gross + fx_impact_mrr_gross) as ending_mrr_gross,
        (c.beginning_mrr_net + c.new_mrr_net + c.expansion_mrr_net + c.downgrade_mrr_net + c.churn_mrr_net + fx_impact_mrr_net) as ending_mrr_net
    from contracts_with_mrr_stg c
), contracts_with_ending_arr as (
    select
        c.*,
        -- Ending ARR
        ending_mrr_gross * 12 as ending_arr_gross,
        ending_mrr_net * 12 as ending_arr_net,
        -- NRR
        case
            when c.reporting_month >= c.contract_start_date and c.type_reporting = 'Event' then c.mrr_gross
            else 0
        end as nrr_gross
    from contracts_with_mrr_totals c
), contracts_with_usage as (
    select
        c.*,
        coalesce(u.total_bytes, 0) as total_bytes,
        coalesce(u.total_rows, 0) as total_rows,
        case
            when c.commit_type in ('TB per Month', 'GB per Month') then coalesce(u.total_bytes, 0)
            else coalesce(u.total_rows, 0)
        end as total_usage_normalized,
        u.cumulative_bytes,
        u.cumulative_rows,
        case
            when c.commit_type in ('TB per Month', 'GB per Month') then u.cumulative_bytes
            else u.cumulative_rows
        end as cumulative_usage_normalized,
        coalesce(u.max_qpm, 0) as max_qpm,
        u.cumulative_max_qpm
    from contracts_with_ending_arr c
    left join {{ref('stg_usage__contract_monthly')}} u
        on c.contract_id = u.contract_id
        and c.reporting_month = u.reporting_month
), contracts_with_grouping_bands as (
    select
        c.*,
        case
            when mrr_gross > 75000 then 'XL'
            when mrr_gross between 25000.0001 and 75000 then 'Large'
            when mrr_gross between 10000 and 25000 then 'Medium'
            else 'Small'
        end as mrr_band,
        case
            when c.commit_type in ('GB per day', 'GB per month', 'TB per month') then
                case
                    when u.total_bytes is null then 'N/A'
                    when u.total_bytes > 570431864 then 'XL'
                    when u.total_bytes between 151694534 and 570431864 then 'Large'
                    when u.total_bytes between 24624909 and 151694534 then 'Medium'
                    else 'Small'
                end
            else
                case
                    when u.total_rows is null then 'N/A'
                    when u.total_rows > 607657130242 then 'XL'
                    when u.total_rows between 141014472467 and 607657130242 then 'Large'
                    when u.total_rows between 22621059000 and 141014472467 then 'Medium'
                    else 'Small'
                end
        end as usage_band
    from contracts_with_usage c
    left join {{ref('stg_monthly_contract_usage')}} u
        on c.contract_id = u.contract_id
        and c.reporting_month = u.reporting_month
)

select * from contracts_with_grouping_bands
