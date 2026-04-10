{{ config(materialized="table") }}

with child_contract_count as (
    select
        master_contract_id,
        count(*) as child_count
    from {{ref('stg_crm__child_contracts')}}
    group by 1
), deployments as (
    select
        d.name as deployment_id,
        coalesce(d.account__c, 'N/A') as account_id,
        coalesce(d.contract__c, 'N/A') as contract_id,
        coalesce(d.opportunity__c, 'N/A') as opportunity_id,
        d.stage__c as stage_name,
        d.created_date,
        d.last_modified_date,
        d.id as salesforce_id,
        left(d.id, 15) as salesforce_short_id,
        d.system_modstamp
    from {{ source('raw_salesforce', 'deployment__c') }} d
    where is_deleted is False
)
select * from deployments
