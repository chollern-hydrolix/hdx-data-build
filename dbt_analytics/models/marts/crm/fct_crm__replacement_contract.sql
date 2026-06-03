{{ config(materialized="table") }}

/*
    Goal: Use the "previous_contract__c" field to establish a "replacement contract" link

    This query de-duplicates any contract that is replaced by more than one contract.
*/

with contract_links as (
    select
        cont.contract_number as contract_number,
        prev_cont.contract_number as previous_contract_number,
        acct.name as account_name,
        cont.start_date as cont_start_date,
        cont.end_date as cont_end_date,
        prev_cont.start_date as prev_cont_start_date,
        prev_cont.end_date as prev_cont_end_date,
        cont.id as contract_id,
        prev_cont.id as previous_contract_id,
        cont.recognition_month__c as cont_activated_effective_month,
        cont.recognition_date__c as cont_activated_effective_date,
        cont.status as cont_status,
        prev_cont.status as previous_contract_status
    from {{ source('raw_salesforce', 'contract') }} cont
    left join {{ source('raw_salesforce', 'contract') }} prev_cont on cont.previous_contract__c = prev_cont.id
    left join {{ source('raw_salesforce', 'account') }} acct on cont.account_id = acct.id
    --where cont.status = 'Activated'
    where (prev_cont.replaced_by_new_contract__c is True or prev_cont.replaced_by_new_draft_contract__c is True)
    and cont.is_deleted is False
    and prev_cont.is_deleted is False
), replacement_contracts as (
    select
        previous_contract_number as contract_number,
        contract_number as replacement_contract_number,
        account_name,
        prev_cont_start_date::date as start_date,
        prev_cont_end_date::date as end_date,
        cont_start_date::date as replacement_start_date,
        cont_end_date::date as replacement_end_date,
        cont_activated_effective_month::date as replacement_activated_effective_month,
        cont_activated_effective_date::date as replacement_activated_effective_date,
        previous_contract_status as contract_status,
        cont_status as replacement_contract_status,
        previous_contract_id as contract_id,
        contract_id as replacement_contract_id
    from contract_links
), replacement_contracts_with_months as (
    select
        *,
        date_trunc('month', start_date)::date as start_month,
        date_trunc('month', end_date)::date as end_month,
        date_trunc('month', replacement_start_date)::date as replacement_start_month,
        date_trunc('month', replacement_end_date)::date as replacement_end_month
    from replacement_contracts
), replacement_contracts_remove_duplicates as (
    select
        *,
        rank() over(
            partition by contract_number
            order by replacement_start_month asc, replacement_contract_number asc
        ) as contract_rank
    from replacement_contracts_with_months
)
select
    contract_number,
    replacement_contract_number,
    account_name,
    start_date,
    end_date,
    replacement_start_date,
    replacement_end_date,
    contract_id,
    replacement_contract_id,
    start_month,
    end_month,
    replacement_start_month,
    replacement_end_month,
    replacement_activated_effective_month,
    replacement_activated_effective_date,
    contract_status,
    replacement_contract_status,
    contract_rank
from replacement_contracts_remove_duplicates
where contract_rank = 1
