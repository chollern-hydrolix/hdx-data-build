{{
    config(
        materialized="table"
    )
}}

/*
IE Bucket enriched with contract, deployment, cluster, account, and opportunity data.
Materialized as a table so downstream models have accurate row statistics.
*/

with ie_bucket_with_contract as (
    select
        ie_b.bucket_name__c as bucket_name,
        ie_b.storage_name__c as storage_name,
        ie_b.id as ie_bucket_id,
        ie_c.name as cluster_hostname,
        coalesce(a.name, a2.name) as account_name,
        d.id as deployment_sfid,
        d.ulid__c as deployment_ulid,
        d.contract__c as contract_id,
        c.contract_number,
        d.contract__c is null as contract_is_null,
        d.opportunity__c is null as opportunity_is_null,
        c.original_contract_start__c as original_contract_start_date,
        d.opportunity__c as opportunity_id,
        o.name as opportunity_name,
        o.close_date as opportunity_close_date,
        o.stage_name as opportunity_stage_name,
        row_number() over (
            partition by ie_b.id
            order by ie_b.last_modified_date
        ) as row_num
    from {{ source('raw_salesforce', 'ie_bucket__c') }} ie_b
    left join {{ source('raw_salesforce', 'ie_table__c') }} ie_t on ie_b.ie_table__c = ie_t.id
    left join {{ source('raw_salesforce', 'ie_project__c') }} ie_p on ie_t.ie_project__c = ie_p.id
    left join {{ source('raw_salesforce', 'ie_cluster__c') }} ie_c on ie_p.ie_cluster__c = ie_c.id
    left join {{ source('raw_salesforce', 'deployment__c') }} d on ie_p.deployment__c = d.id
    left join {{ source('raw_salesforce', 'contract') }} c on d.contract__c = c.id
    left join {{ source('raw_salesforce', 'account') }} a on c.account_id = a.id
    left join {{ source('raw_salesforce', 'opportunity') }} o on d.opportunity__c = o.id
    left join {{ source('raw_salesforce', 'account') }} a2 on o.account_id = a2.id
)
select *
from ie_bucket_with_contract
where row_num = 1
