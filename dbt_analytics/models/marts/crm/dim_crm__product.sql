{{config(materialized="table")}}

with salesforce_products as (
    select
        p.id as product_id,
        p.name as product_name,
        p.family as product_family,
        p.revenue_type__c as revenue_type,
        p.is_active as is_active,
        p.is_archived as is_archived,
        p.created_date,
        p.last_modified_date,
        left(p.id, 15) as contract_short_id
    from {{ source('raw_salesforce', 'product2') }} p
    where is_deleted is False
)
select * from salesforce_products
