{{ config(materialized="table") }}

with ie_shared as (
    select
        ie_c.name as cluster_name,
        ie_p.name as project_name,
        ie_p.cluster_project__c as infrastructure_element_name,
        ie_p.deployment__c as deployment_sfid,
        'project' as ie_type
    from {{ source('raw_salesforce', 'ie_project__c') }} ie_p
    left join {{ source('raw_salesforce', 'ie_cluster__c') }} ie_c on ie_p.ie_cluster__c = ie_c.id
    where ie_p.is_deleted is False
    and (
        ie_c.hdx_shared_cluster__c is True or
        ie_c.multi_tenant_cluster__c is True
    )
), ie_dedicated as (
    select
        ie_c.name as cluster_name,
        'N/A' as project_name,
        ie_c.name as infrastructure_element_name,
        ie_c.deployment__c as deployment_sfid,
        'cluster' as ie_type
    from {{ source('raw_salesforce', 'ie_cluster__c') }} ie_c
    where ie_c.is_deleted is False
    and ie_c.hdx_shared_cluster__c is False
    and ie_c.multi_tenant_cluster__c is False
), ie_union as (
    select * from ie_shared
        union all
    select * from ie_dedicated
), ie_with_deployment as (
    select
        ie.cluster_name,
        ie.project_name,
        ie.infrastructure_element_name,
        ie.ie_type,
        ie.deployment_sfid,
        d.deployment_id as deployment_ulid,
        d.contract_id,
        d.opportunity_id,
        d.stage_name
    from ie_union ie
    left join {{ ref('fct_crm__deployment') }} d on ie.deployment_sfid = d.salesforce_id
)
select * from ie_with_deployment
