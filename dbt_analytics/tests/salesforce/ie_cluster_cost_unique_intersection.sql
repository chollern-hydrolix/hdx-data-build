with stg as (
    select
        ie_cluster__c,
        invoice_date__c,
        count(*) as _count
    from {{ source('raw_salesforce', 'ie_cluster_cost__c') }}
    where is_deleted is False
    group by 1, 2
)
select * from stg where _count > 1
