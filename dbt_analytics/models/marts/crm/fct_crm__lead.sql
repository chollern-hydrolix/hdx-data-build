{{config(materialized="table")}}

with salesforce_leads as (
    select
        l.id as lead_id,
        l.name,
        l.title,
        l.company,
        rt.name as lead_record_type,
        l.status,
        coalesce(u1.name, 'N/A') as lead_owner,
        coalesce(l.region__c, 'N/A') as region,
        coalesce(l.channel__c, 'N/A') as channel,
        coalesce(l.lead_source, 'N/A') as lead_source,
        coalesce(l.lead_source_details__c, 'N/A') as lead_source_details,
        coalesce(l.lead_source_note__c, 'N/A') as lead_source_note,
        coalesce(l.industry, 'N/A') as industry,
        coalesce(l.sub_industry__c, 'N/A') as sub_industry,
        l.last_activity_date as last_activity_date,
        coalesce(bdr.name, 'N/A') as bdr_qualified_by,
        coalesce(l.bdr_notes__c, 'N/A') as bdr_notes,
        l.duration_in_prospect__c as duration_in_prospect,
        l.duration_in_mql__c as duration_in_mql,
        l.duration_in_sql__c as duration_in_sql,
        l.entered_prospect__c as entered_prospect,
        l.entered_converted__c as entered_converted,
        l.entered_disqualified__c as entered_disqualified,
        l.entered_nurture__c as entered_nurture,
        l.entered_mql__c as entered_mql,
        l.entered_sql__c as entered_sql,
        l.created_date as created_date,
        created_by.name as created_by,
        l.last_modified_date as last_modified_date,
        left(l.id, 15) as lead_short_id,
        l.system_modstamp
    from {{ source('raw_salesforce', 'lead') }} l
    left join {{ source('raw_salesforce', 'user') }} u1 on l.owner_id = u1.id
    left join {{ source('raw_salesforce', 'user') }} bdr on l.bdr_qualified_by__c = bdr.id
    left join {{ source('raw_salesforce', 'user') }} created_by on l.created_by_id = created_by.id
    left join {{ source('raw_salesforce', 'record_type') }} rt on l.record_type_id = rt.id
    where is_deleted is False
)
select * from salesforce_leads
