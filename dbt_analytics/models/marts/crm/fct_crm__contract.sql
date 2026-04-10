{{config(materialized="table")}}

with salesforce_contracts as (
    select
        c.id as contract_id,
        c.contract_number as contract_number,
        c.start_date as contract_start_date,
        c.end_date as contract_end_date,
        c.contract_term as contract_term,
        c.status as status,
        c.type__c as type,
        c.type_calculated__c as type_calculated,
        c.type_reporting__c as type_reporting,
        c.replaced_by_new_contract__c as replaced_by_new_contract,
        c.previous_contract__c as previous_contract_id,
        c.account_id as account_id,
        c.channel__c as channel,
        c.hydrolix_product__c as hydrolix_product,
        c.region__c as region,
        c.country__c as country,
        c.recognition_date__c as activated_effective_date,
        c.recognition_month__c as activated_effective_month,
        -- Reporting Start Date: Used to ensure there are not overlapping contracts
        case
            -- If this contract replaced another mid-contract period
            when c.previous_contract_end_date__c > c.start_date then c.recognition_date__c::date
            -- Handle activated effective month following the contract duration
            when c.type_calculated__c = 'New Business' and c.recognition_date__c > c.end_date then c.start_date::date
            -- If New Business, Cancellation, or Expiration, use activated effective month
            when c.type_calculated__c in ('New Business', 'Cancellation', 'Expiration') then c.recognition_date__c::date
            -- Otherwise, use activated_effective_month
            else c.recognition_date__c::date
        end as reporting_start_date,
        -- If this contract was replaced by another mid-contract period: use the month prior to the replacement start month to end this contract
        case
            -- If replacement contract exists: run this contract through the month preceding the replacement contract activated effective month
            when repl_c.replacement_activated_effective_date is not null then (repl_c.replacement_activated_effective_date - interval '1 day')::date
            -- If type is Cancellation or Expiration: make sure end_month is on or after activated_effective_month
            when c.type_calculated__c in ('Cancellation', 'Expiration') and c.recognition_date__c > c.end_date then c.recognition_date__c::date
            -- Otherwise, use end_month
            else c.end_date::date
        end as reporting_end_date,
        coalesce(c.churn_confirmed_date__c, DATE '2099-12-31')::date as churn_confirmed_date,
        coalesce(c.churn_confirmed_date_month__c, DATE '2099-12-31')::date as churn_confirmed_month,
        c.mrr__c as mrr_gross,
        c.mrr_net__c as mrr_net,
        c.gmrr_gross_2025__c as gmrr_gross,
        c.gmrr_net_2025__c as gmrr_net,
        c.lmrr_gross_2025__c as lmrr_gross,
        c.lmrr_net_2025__c as lmrr_net,
        c.tcv_net__c as tcv_net,
        c.arr__c as arr,
        c.nrr_gross_2025__c as nrr_gross_2025,
        c.event_nrr_gross_2025__c as event_nrr_gross_2025,
        tb_per_month_standard__c as tb_per_month_standard,
        tb_per_month_premium__c as tb_per_month_premium,
        (tb_per_month_standard__c * (1024^4)) as bytes_per_month_standard,
        (tb_per_month_premium__c * (1024^4)) as bytes_per_month_premium,
        standard_overages__c as standard_overages_per_gb,
        premium_overages__c as premium_overages_per_gb,
        raw_log_retention_standard__c as raw_log_retention_standard,
        summary_log_retention_standard__c as summary_log_retention_standard,
        c.commit_amount__c as commit_amount,
        c.commit_type__c as commit_type,
        coalesce(c.commit_months__c, 0) as commit_months,
        coalesce(c.commit_amount_premium__c, 0) as commit_amount_premium,
        coalesce(c.standard_overages__c, 0) as standard_overages,
        coalesce(c.premium_overages__c, 0) as premium_overages,
        coalesce(c.retention_raw_data__c, 0) as retention_raw_data,
        coalesce(c.retention_summary_data__c, 0) as retention_summary_data,
        coalesce(c.peak_queries_per_minute__c, 0) as peak_queries_per_minute,
        case
            when c.commit_type__c = 'Billion records per month' then c.commit_amount__c * (10^9)
            -- when c.commit_type__c = 'TB per Month' then c.commit_amount__c * (1024^4)
            -- when c.commit_type__c = 'GB per month' then c.commit_amount__c * (1024^3)
            else 0
        end as commit_normalized,
        usage_based_billing_confirmed__c as usage_based_billing_confirmed,
        date_trunc('month', c.start_date)::date as contract_start_month,
        date_trunc('month', c.end_date)::date as contract_end_month,
        contract_active__c as contract_active,
        fx_rate__c as fx_rate,
        fx_impact_mrr__c as fx_impact_mrr,
        coalesce(c.customer_count__c, 0) as customer_count_impact,
        coalesce(akamai_contract_id__c, 'N/A') as akamai_contract_id,
        coalesce(pricing_calculator_version__c, 0) as pricing_calculator_version,
        coalesce(original_contract_start__c, '9999-12-31'::date) as original_contract_start_date,
        c.has_overages__c as has_overages,
        c.overage_charges__c as overage_charges,
        c.created_date,
        c.last_modified_date,
        left(c.id, 15) as contract_short_id,
        c.system_modstamp
    from {{source('raw_salesforce', 'contract')}} c
    left join {{ ref('fct_crm__replacement_contract') }} repl_c on c.id = repl_c.contract_id
    where c.is_deleted is False
)
select * from salesforce_contracts
