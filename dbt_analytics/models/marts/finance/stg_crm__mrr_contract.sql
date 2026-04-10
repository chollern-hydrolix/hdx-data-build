{{ config(materialized="view") }}

with contracts as (
    select
        c.start_date,
        c.end_date,
        c.contract_term,
        c.mrr_net__c as mrr_net,
        c.mrr__c as mrr_gross,
        coalesce(contract_override.gmrr_gross, c.gmrr_gross_2025__c) as gmrr_gross,
        coalesce(contract_override.gmrr_net, c.gmrr_net_2025__c) as gmrr_net,
        c.lmrr_gross_2025__c as lmrr_gross,
        c.lmrr_net_2025__c as lmrr_net,
        c.arr__c as arr,
        c.contract_number as contract_number,
        c.tcv__c as tcv,
        c.churn_date_reporting__c as churn_date_reporting,
        c.churn_date_reporting_month__c as churn_date_reporting_month,
        c.churn_date_reporting_quarter__c as churn_date_reporting_quarter,
        c.finance_reporting_date__c as finance_reporting_date,
        coalesce(contract_override.type_calculated, c.type_calculated__c) as type_calculated,
        c.channel__c as channel,
        c.hydrolix_product__c as hydrolix_product,
        c.region__c as region,
        c.country__c as country,
        c.status as status,
        c.type__c as type,
        a.name as account_name,
        a.id as account_id,
        c.id as contract_id,
        c.recognition_date__c as activated_effective_date,
        c.recognition_month__c as activated_effective_month,
        date_trunc('month', c.start_date) as start_month,
        date_trunc('month', c.end_date) as end_month,
        c.churn_mrr_gross_2025__c as churn_mrr_gross_2025,
        c.churn_mrr_net_2025__c as churn_mrr_net_2025,
        c.churn_mrr_net__c as churn_mrr_net,
        date_trunc('month', c.previous_contract_start_date__c)::date as previous_contract_start_month,
        date_trunc('month', c.previous_contract_end_date__c)::date as previous_contract_end_month,
        c.previous_contract__c as previous_contract_id,
        c.replaced_by_new_contract__c as replaced_by_new_contract,
        c.bridge_renewal__c as is_bridge_renewal,
        c.replaced_by_new_draft_contract__c as replaced_by_draft_contract,
        c.commit_amount__c as commit_amount,
        c.commit_type__c as commit_type,
        c.type_reporting__c as type_reporting,
        c.fx_impact_mrr__c as fx_impact_mrr,
        c.fx_rate__c as fx_rate,
        c.customer_count__c as customer_count_impact,
        c.churn_confirmed__c as churn_confirmed,
        c.churn_confirmed_date__c as churn_confirmed_date,
        c.opportunity__c as opportunity_id,
        u1.name as sold_by,
        c.contract_active__c as contract_active,
        a.industry as industry,
        a.primary_industry__c as primary_industry,
        a.primary_sub_industry__c as primary_sub_industry,
        c.akamai_sales_rep__c as akamai_sales_rep
    from {{source('raw_salesforce', 'contract')}} c
    left join {{source('raw_salesforce', 'account')}} a on c.account_id = a.id
    left join {{source('raw_salesforce', 'user')}} u1 on c.sold_by__c = u1.id
    left join static_mapping.contract_override on c.id = contract_override.contract_id
    where c.is_deleted is False
    and a.name not ilike '%123test%'
    and a.name not ilike '%456test%'
    and a.name is not null
    and c.type__c in (
        'Cancellation', 'Downgrade', 'Event', 'Expiration',
        'Extend', 'New Business', 'Renew', 'Upgrade',
        'High Traffic Event'
    )
)
select * from contracts
