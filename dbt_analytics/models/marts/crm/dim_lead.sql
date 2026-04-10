{{
    config(
        materialized="table"
    )
}}

with salesforce_leads as (
    select
        l.name as lead_name,
        l.id as lead_id,
        coalesce(l.lead_source, 'N/A') as lead_source,
        coalesce(l.status, 'N/A') as status,
        coalesce(l.industry, 'N/A') as industry,
        coalesce(l.rating, 'N/A') as rating,
        coalesce(l.region__c, 'N/A') as region,
        coalesce(l.vertical__c, 'N/A') as vertical,
        coalesce(l.channel__c, 'N/A') as channel,
        coalesce(l.country__c, 'N/A') as country,
        l.salutation as salutation,
        l.first_name as first_name,
        l.middle_name as middle_name,
        l.last_name as last_name,
        l.suffix as suffix,
        l.title as title,
        l.company as company,
        l.email as email,
        l.website as website,
        u1.name as owner_name,
        u2.name as created_by_name,
        l.is_converted as is_converted,
        coalesce(l.converted_account_id, 'N/A') as converted_account_id,
        coalesce(l.converted_opportunity_id, 'N/A') as converted_opportunity_id,
        coalesce(l.converted_contact_id, 'N/A') as converted_contact_id,
        l.lead_source_details__c as lead_source_details,
        coalesce(l.use_case__c, 'N/A') as use_case,
        coalesce(l.use_case_other_specify__c, 'N/A') as use_case_other_specify,
        coalesce(l.disqualification_reason__c, 'N/A') as disqualification_reason,
        coalesce(l.disqualification_reason_other_specify__c, 'N/A') as disqualification_reason_other_specify,
        l.entered_unqualified__c as entered_unqualified,
        l.entered_prospect__c as entered_prospect,
        l.entered_m_q_l__c as entered_mql,
        l.entered_s_q_l__c as entered_sql,
        l.entered_disqualified__c as entered_disqualified,
        l.entered_converted__c as entered_converted,
        l.duration_in_unqualified__c as duration_in_unqualified,
        l.duration_in_prospect__c as duration_in_prospect,
        l.duration_in_m_q_l__c as duration_in_mql,
        l.duration_in_s_q_l__c as duration_in_sql,
        l.total_lead_cycle_duration__c as total_lead_cycle_duration,
        l.lead_temperature__c as lead_temperature,
        l.hubspot_i_d__c as hubspot_id,
        replace(l.lead_record_type_developer_name__c, '_', ' ') as record_type_name,
        l.last_activity_date,
        l.last_viewed_date,
        l.created_date,
        l.last_modified_date,
        left(l.id, 15) as lead_short_id
    from salesforce.lead l
    left join salesforce.user u1 on l.owner_id = u1.id
    left join salesforce.user u2 on l.created_by_id = u2.id
    where is_deleted is False
)
select * from salesforce_leads
