{{
    config(
        materialized="table"
    )
}}

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
        c.churn_confirmed_date__c as churn_confirmed_date,
        c.churn_confirmed_date_month__c as churn_confirmed_month,
        c.m_r_r__c as mrr_gross,
        c.m_r_r_net__c as mrr_net,
        c.g_m_r_r_gross_2025__c as gmrr_gross,
        c.g_m_r_r_net_2025__c as gmrr_net,
        c.l_m_r_r_gross_2025__c as lmrr_gross,
        c.l_m_r_r_net_2025__c as lmrr_net,
        c.t_c_v_net__c as tcv_net,
        c.a_r_r__c as arr,
        c.n_r_r_gross_2025__c as nrr_gross_2025,
        c.event_n_r_r_gross_2025__c as event_nrr_gross_2025,
        c.commit_amount__c as commit_amount,
        c.commit_type__c as commit_type,
        case
            when c.commit_type__c = 'Billion records per month' then c.commit_amount__c * (10^9)
            when c.commit_type__c = 'TB per Month' then c.commit_amount__c * (1024^4)
            when c.commit_type__c = 'GB per month' then c.commit_amount__c * (1024^3)
            else 0
        end as commit_normalized,
        t_b_per_month_standard__c as tb_per_month_standard,
        t_b_per_month_premium__c as tb_per_month_premium,
        (t_b_per_month_standard__c * (1024^4)) as bytes_per_month_standard,
        (t_b_per_month_premium__c * (1024^4)) as bytes_per_month_premium,
        standard_overages__c as standard_overages_per_gb,
        premium_overages__c as premium_overages_per_gb,
        usage_based_billing_confirmed__c as usage_based_billing_confirmed,
        date_trunc('month', c.start_date)::date as contract_start_month,
        date_trunc('month', c.end_date)::date as contract_end_month,
        contract_active__c as contract_active,
        f_x_rate__c as fx_rate,
        f_x_impact_m_r_r__c as fx_impact_mrr,
        coalesce(c.customer_count__c, 0) as customer_count_impact,
        coalesce(akamai_contract_i_d__c, 'N/A') as akamai_contract_id,
        coalesce(pricing_calculator_version__c, 0) as pricing_calculator_version,
        c.has_overages__c as has_overages,
        c.overage_charges__c as overage_charges,
        c.created_date,
        c.last_modified_date,
        left(c.id, 15) as contract_short_id
    from salesforce.contract c
    where c.is_deleted is False
)
select * from salesforce_contracts
