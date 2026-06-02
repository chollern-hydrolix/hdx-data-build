{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['allocation_month', 'cluster_hostname']},
            {'columns': ['deployment_sfid']}
        ]
    )
}}

with months as (
    select month_date as allocation_month
    from {{ ref('dim_month') }}
    where month_date between '2025-01-01' and current_date
), ie_clusters as (
    select distinct
        name as cluster_hostname,
        deployment__c as deployment_sfid
    from {{ source('raw_salesforce', 'ie_cluster__c') }}
    where is_deleted is false
      and deployment__c is not null
), ie_projects as (
    select distinct
        cluster_project__c as cluster_project_name,
        deployment__c as deployment_sfid
    from {{ source('raw_salesforce', 'ie_project__c') }}
    where is_deleted is false
      and deployment__c is not null
), shared_alc as (
    select
        month as allocation_month,
        cluster_hostname,
        cluster_project_name,
        sum(monthly_pro_rated_pct) as pro_rated_pct
    from {{ ref('stg_linode__monthly_shared_cluster_allocation') }}
    group by 1, 2, 3
), shared_mapping as (
    select
        a.allocation_month,
        a.cluster_hostname,
        a.cluster_project_name,
        p.deployment_sfid,
        a.pro_rated_pct,
        'shared'::text as allocation_type
    from shared_alc a
    inner join ie_projects p on a.cluster_project_name = p.cluster_project_name
), shared_clusters_per_month as (
    select distinct allocation_month, cluster_hostname
    from shared_mapping
), dedicated_mapping as (
    select
        m.allocation_month,
        c.cluster_hostname,
        null::text as cluster_project_name,
        c.deployment_sfid,
        1.0::numeric as pro_rated_pct,
        'dedicated'::text as allocation_type
    from ie_clusters c
    cross join months m
    where not exists (
        select 1 from shared_clusters_per_month s
        where s.allocation_month = m.allocation_month
          and s.cluster_hostname = c.cluster_hostname
    )
)
select
    {{ dbt_utils.generate_surrogate_key([
        'allocation_month',
        'cluster_hostname',
        'coalesce(cluster_project_name, \'\')',
        'deployment_sfid'
    ]) }} as cluster_to_deployment_id,
    allocation_month,
    cluster_hostname,
    cluster_project_name,
    deployment_sfid,
    pro_rated_pct,
    allocation_type
from dedicated_mapping
union all
select
    {{ dbt_utils.generate_surrogate_key([
        'allocation_month',
        'cluster_hostname',
        'coalesce(cluster_project_name, \'\')',
        'deployment_sfid'
    ]) }} as cluster_to_deployment_id,
    allocation_month,
    cluster_hostname,
    cluster_project_name,
    deployment_sfid,
    pro_rated_pct,
    allocation_type
from shared_mapping