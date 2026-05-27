{{ config(materialized="table") }}


with notes as (
    select *
    from {{ source('raw_salesforce', 'note') }}
    where is_deleted is False
), notes_with_people as (
    select
        n.title,
        n.body,
        u1.name as owner_name,
        u2.name as created_by_name,
        u3.name as last_modified_by_name,
        n.created_date,
        n.last_modified_date,
        n.id as note_id,
        n.parent_id,
        n.owner_id,
        n.created_by_id,
        n.last_modified_by_id,
        left(n.parent_id, 3) as parent_prefix
    from notes n
    left join {{ source('raw_salesforce', 'user') }} u1 on n.owner_id = u1.id
    left join {{ source('raw_salesforce', 'user') }} u2 on n.created_by_id = u2.id
    left join {{ source('raw_salesforce', 'user') }} u3 on n.last_modified_by_id = u3.id
), account_notes as (
    select
        n.*,
        'Account' as parent_type,
        a.account_name as account_name,
        a.account_id as account_id
    from notes_with_people n
    left join {{ ref('dim_crm__account') }} a on n.parent_id = a.account_id
    where parent_prefix = '001'
), other_notes as (
    select
        n.*,
        'Other' as parent_type,
        'N/A' as account_name,
        'N/A' as account_id
    from notes_with_people n
    where parent_prefix != '001'
), union_notes as (
    select * from account_notes
        union all
    select * from other_notes
)
select * from union_notes
