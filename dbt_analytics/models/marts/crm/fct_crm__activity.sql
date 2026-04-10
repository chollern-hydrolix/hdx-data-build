{{
    config(
        materialized="table",
        indexes=[
            {'columns': ['activity_date']}
        ]
    )
}}

with events as (
    select
        id as activity_id,
        'Event' as activity_type,
        event_subtype as activity_subtype,
        owner_id,
        who_id,
        what_id,
        account_id,
        subject,
        description,
        activity_date,
        created_date,
        last_modified_date,
        activity_date_time,
        start_date_time,
        end_date_time,
        null::timestamp as completed_date_time,
        null as status,
        null as priority,
        null::bool as is_closed,
        is_archived,
        db_activity_type__c as db_activity_type,
        gong__gong_activity_id__c as gong_activity_id,
        gong__gong_participants_emails__c as gong_participants_emails
    from {{(source('raw_salesforce', 'event'))}}
    where is_deleted is False
), tasks as (
    select
        id as activity_id,
        'Task' as activity_type,
        task_subtype as activity_subtype,
        owner_id,
        who_id,
        what_id,
        account_id,
        subject,
        description,
        activity_date,
        created_date,
        last_modified_date,
        null::timestamp as activity_date_time,
        null::timestamp as start_date_time,
        null::timestamp as end_date_time,
        completed_date_time,
        status,
        priority,
        is_closed,
        is_archived,
        db_activity_type__c as db_activity_type,
        gong__gong_activity_id__c as gong_activity_id,
        gong__gong_participants_emails__c as gong_participants_emails
    from {{source('raw_salesforce', 'task')}}
    where is_deleted is False
), union_data as (
    select * from events
        union all
    select * from tasks
), data_with_related_records as (
    select
        u.*,
        case
            when who_id is null then 'N/A'
            when who_lead.name is null then 'Contact'
            else 'Lead'
        end as person_type,
        coalesce(who_lead.name, who_contact.name, 'N/A') as person_name,
        case
            when what_account.id is not null then 'Account'
            when what_opportunity.id is not null then 'Opportunity'
            when what_case.id is not null then 'Case'
            else 'N/A'
        end as record_type,
        case
            when what_account.name is not null then what_account.name
            when what_opportunity.name is not null then what_opportunity.name
            when what_case.case_number is not null then concat('CASE ', what_case.case_number)
            else 'N/A'
        end as record_name,
        coalesce(owner.name, 'N/A') as owner_name,
        coalesce(account.name, 'N/A') as account_name,
        who_lead.id as lead_id,
        who_contact.id as contact_id,
        what_opportunity.id as opportunity_id,
        what_case.id as case_id
    from union_data u
    -- WhoId Joins
    left join {{source('raw_salesforce', 'lead')}} who_lead on u.who_id = who_lead.id
    left join {{source('raw_salesforce', 'contact')}} who_contact on u.who_id = who_contact.id
    -- WhatId Joins
    left join {{source('raw_salesforce', 'account')}} what_account on u.what_id = what_account.id
    left join {{source('raw_salesforce', 'opportunity')}} what_opportunity on u.what_id = what_opportunity.id
    left join {{source('raw_salesforce', 'case')}} what_case on u.what_id = what_case.id
    -- Owner and Account Joins
    left join {{source('raw_salesforce', 'user')}} owner on u.owner_id = owner.id
    left join {{source('raw_salesforce', 'account')}} account on u.account_id = account.id
), final_data as (
    select
        activity_id,
        activity_type,
        activity_subtype,
        account_name,
        owner_name
        person_name,
        person_type,
        subject,
        description,
        activity_date,
        created_date,
        last_modified_date,
        activity_date_time,
        start_date_time,
        end_date_time,
        completed_date_time,
        status,
        priority,
        is_closed,
        is_archived,
        db_activity_type,
        gong_activity_id,
        gong_participants_emails,
        account_id
        owner_id,
        who_id,
        what_id,
        lead_id,
        contact_id,
        opportunity_id,
        case_id
    from data_with_related_records
)
select * from final_data
