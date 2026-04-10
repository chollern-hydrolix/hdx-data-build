{{config(materialized="view")}}

with fleet_projects as (
    select
        project_uuid::text,
        project_deployment_id
    from argus.fleet_project
), ie_projects as (
    select
        p.project_uuid__c::text as project_uuid,
        p.deployment__c as salesforce_deployment_id,
        d.ulid__c as deployment_id
    from raw_salesforce.ie_project__c p
    left join raw_salesforce.deployment__c d on p.deployment__c = d.id
    where p.is_deleted is False
    and d.is_deleted is False
), final_data as (
    select
        ie.deployment_id as deployment_ulid,
        project_deployment_id as fleet_project_deployment_id,
        p.project_uuid as project_uuid
    from ie_projects ie
    left join fleet_projects p on ie.project_uuid = p.project_uuid
)
select * from final_data where deployment_ulid is not null
