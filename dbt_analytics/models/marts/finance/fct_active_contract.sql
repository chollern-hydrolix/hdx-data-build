{{ config(materialized="table") }}

/*
*/

with contracts as (
    select
        id as contract_id,
        contract_number,
        start_date,
        end_date,
        status,
        type__c as type,
        type_calculated__c as type_calculated,
        type_reporting__c as type_reporting,
        m_r_r__c as mrr_gross,
        m_r_r_net__c as mrr_net,
        g_m_r_r_gross_2025__c as gmrr_gross,
        g_m_r_r_net_2025__c as gmrr_net,
        l_m_r_r_gross_2025__c as lmrr_gross,
        l_m_r_r_net_2025__c as lmrr_net,
        not(replaced_by_new_contract__c) as is_expiration_risk,
        account_id,
        channel__c as channel,
        region__c as region,
        country__c as country,
        hydrolix_product__c as hydrolix_product
    from salesforce.contract
    where contract_active__c is True
    and is_deleted is False
    and type_calculated__c not in ('Cancellation', 'Expiration', 'Event')
), contracts_with_account as (
    select
        c.*,
        a.name as account_name
    from contracts c
    left join salesforce.account a on c.account_id = a.id
), contracts_with_months as (
    select
        c.*,
        date_trunc('month', start_date) as start_month,
        date_trunc('month', end_date) as end_month,
        date_trunc('month', end_date + interval '1 month') as expiration_month
    from contracts_with_account c
), contracts_with_churn_amount as (
    select
        c.*,
        -mrr_gross as mrr_gross_expiration_risk,
        -mrr_net as mrr_net_expiration_risk
    from contracts_with_months c
)
select
    account_name,
    contract_number,
    start_date,
    end_date,
    status,
    type,
    type_calculated,
    type_reporting,
    channel,
    region,
    country,
    hydrolix_product,
    mrr_gross,
    mrr_net,
    gmrr_gross,
    gmrr_net,
    lmrr_gross,
    lmrr_net,
    mrr_gross_expiration_risk,
    mrr_net_expiration_risk,
    expiration_month as activated_effective_month,
    expiration_month as churn_month,
    expiration_month as reporting_month,
    start_month,
    end_month,
    expiration_month,
    account_id,
    contract_id,
    is_expiration_risk
from contracts_with_churn_amount
