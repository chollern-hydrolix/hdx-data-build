with stg as (
    select
        ie_table__c,
        invoice_month__c,
        count(*) as _count
    from {{ source('raw_salesforce', 'ie_table_usage__c') }}
    where is_deleted is False
    group by 1, 2
)
select * from stg where _count > 1
