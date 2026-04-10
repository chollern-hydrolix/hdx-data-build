{{ config(materialized="view") }}

with contracts as (
    select
        master.contract_number as master_contract_number,
        master.id as master_contract_id,
        child.contract_number as child_contract_number,
        child.id as child_contract_id
    from raw_salesforce.contract master
    left join raw_salesforce.contract child on master.id = child.master_contract__c
    where child.id is not null
    and master.is_deleted is False
)
select * from contracts
