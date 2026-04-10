{{
    config(
        materialized="table"
    )
}}

with salesforce_hdx_deployments as (
    select
        d.id as deployment_id,
        d.name as hdx_deployment_name,
        d.cluster_hostname__c as cluster_hostname,
        d.cluster_name__c as cluster_project_name,
        d.cluster_project_name_calculated__c as cluster_project_name_calculated,
        d.project_name__c as project_name,
        d.status__c as status,
        d.type__c as type,
        d.sales_region__c as sales_region,
        d.cost_type__c as cost_type,
        d.hydrolix_product__c as hydrolix_product,
        d.p_o_c__c as poc,
        d.account__c as account_id,
        d.contract__c as contract_id,
        d.opportunity__c as opportunity_id,
        d.cluster_creation_date__c as cluster_creation_date,
        d.cluster_type__c as cluster_type,
        d.cluster_type_calculated__c as cluster_type_calculated,
        d.cluster_cloud__c as cluster_cloud,
        d.cluster_label__c as cluster_label,
        d.last_verified__c as last_verified,
        d.last_active_in_argus_date__c as last_active_in_argus_date,
        d.last_p_o_c_credit_month__c as last_poc_credit_month,
        d.target_p_o_c_retirement_date__c as target_poc_retirement_date,
        d.target_production_retirement_date__c as target_production_retirement_date,
        d.target_retirement_date__c as target_retirement_date,
        concat(d.cluster_hostname__c, '-', d.project_name__c) as default_deployment_id,
        coalesce(d.argus_ingest_t_b_last_30_days__c, 0) as argus_ingest_tb_last_30_days,
        d.grafana_u_r_l__c as grafana_url,
        regexp_replace(
            regexp_replace(d.cluster_hostname__c, '^https?://', ''),
            '/.*$', ''
        ) as clean_cluster_hostname,
        d.created_date,
        d.last_modified_date,
        left(d.id, 15) as deployment_short_id,
        a.name = 'INTERNAL' as is_internal_account
    from salesforce.hdx_deployment d
    left join salesforce.account a on d.account__c = a.id
    where d.is_deleted is False
    and a.name not in ('123TEST', '456TEST')
)
select * from salesforce_hdx_deployments
