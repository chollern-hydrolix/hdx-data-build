{{config(materialized="table")}}

with salesforce_cases as (
    select
        c.id as case_id,
        c.case_number as case_number,
        a.account_name,
        contact.name as contact_name,
        u1.name as owner_name,
        c.subject as subject,
        c.type as type,
        c.status as status,
        c.reason as reason,
        c.origin as origin,
        c.priority as priority,
        c.closed_date as closed_date,
        coalesce(c.issue_type__c, 'N/A') as issue_type,
        coalesce(c.customer_priority__c, 'N/A') as customer_priority,
        c.is_closed as is_closed,
        c.is_escalated as is_escalated,
        coalesce(c.supplied_name, 'N/A') as supplied_name,
        coalesce(c.supplied_email, 'N/A') as supplied_email,
        coalesce(c.supplied_phone, 'N/A') as supplied_phone,
        coalesce(c.supplied_company, 'N/A') as supplied_company,
        c.contact_email as contact_email,
        c.contact_phone as contact_phone,
        c.master_record_id as master_record_id,
        c.account_id as account_id,
        c.contact_id as contact_id,
        c.owner_id,
        c.created_by_id,
        c.last_modified_by_id,
        c.created_date,
        c.last_modified_date,
        left(c.id, 15) as contract_short_id,
        c.system_modstamp
    from {{source('raw_salesforce', 'case')}} c
    left join {{source('raw_salesforce', 'user')}} u1 on c.owner_id = u1.id
    left join {{ref('dim_crm__account')}} a on c.account_id = a.account_id
    left join {{source('raw_salesforce', 'contact')}} contact on c.contact_id = contact.id
    where c.is_deleted is False
)
select * from salesforce_cases
