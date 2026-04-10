{{
    config(
        materialized="table"
    )
}}

/*

Joins Azure costs with Salesforce IE Buckets

*/

with azure_costs as (
    select
        billing_month as billing_month,
        resource_name as bucket_name,
        sum(pre_tax_cost) as pre_tax_cost
    from azure.monthly_cost_fact
    group by 1, 2
), ie_bucket_raw as (
    select
        id as ie_bucket_id,
        storage_name__c as sf_storage_name,
        bucket_name__c as sf_bucket_name,
        row_number() over (
            partition by bucket_uuid__c
            order by system_modstamp desc, id desc
        ) as rn
    from raw_salesforce.ie_bucket__c
    where is_deleted is False
    and bucket_cloud__c ilike '%azure%'
), ie_bucket as (
    select
        ie_bucket_id,
        sf_storage_name,
        sf_bucket_name
    from ie_bucket_raw
    where rn = 1

-- Unpivot storage_name and bucket_name into a single lookup key,
-- then deduplicate so no bucket name string appears more than once.
-- This prevents fan-out when joining to Azure costs.
), ie_bucket_unpivoted as (
    select sf_storage_name as lookup_key, ie_bucket_id, 1 as priority
    from ie_bucket
    where sf_storage_name is not null

    union all

    select sf_bucket_name as lookup_key, ie_bucket_id, 2 as priority
    from ie_bucket
    where sf_bucket_name is not null
), ie_bucket_ranked as (
    select
        lookup_key,
        ie_bucket_id,
        row_number() over (partition by lookup_key order by priority, ie_bucket_id) as rn
    from ie_bucket_unpivoted
), ie_bucket_deduped as (
    select
        lookup_key,
        ie_bucket_id
    from ie_bucket_ranked
    where rn = 1
), azure_joined as (
    select
        ac.billing_month,
        ac.pre_tax_cost,
        ac.bucket_name,
        bd.ie_bucket_id
    from azure_costs ac
    left join ie_bucket_deduped bd on ac.bucket_name = bd.lookup_key
), azure_data_invoice_staged as (
    select
        a.billing_month as invoice_month,
        a.pre_tax_cost as azure_cost,
        coalesce(ie_b.ie_bucket_id, 'N/A') as ie_bucket_id,
        coalesce(ie_b.storage_name, ie_b.bucket_name, a.bucket_name) as bucket_name,
        case
            when ie_b.opportunity_is_null is False and ie_b.contract_is_null then 'POC'
            when ie_b.contract_is_null is False and ie_b.original_contract_start_date > a.billing_month then 'POC'
            when ie_b.contract_is_null is False and ie_b.original_contract_start_date <= a.billing_month then 'PAID'
            else 'INTERNAL'
        end as cost_type,
        coalesce(ie_b.cluster_hostname, 'N/A') as cluster_hostname,
        coalesce(ie_b.account_name, 'N/A') as account_name,
        coalesce(ie_b.opportunity_name, 'N/A') as opportunity_name,
        coalesce(ie_b.contract_number, 'N/A') as contract_number,
        coalesce(ie_b.opportunity_stage_name, 'N/A') as opportunity_stage_name,
        coalesce(ie_b.opportunity_close_date, date '9999-12-31') as opportunity_close_date,
        coalesce(ie_b.deployment_ulid, 'N/A') as deployment_ulid,
        coalesce(ie_b.deployment_sfid, 'N/A') as deployment_sfid,
        coalesce(ie_b.opportunity_id, 'N/A') as opportunity_id,
        coalesce(ie_b.contract_id, 'N/A') as contract_id
    from azure_joined a
    left join {{ ref('int_cogs__ie_bucket_with_contract') }} ie_b on a.ie_bucket_id = ie_b.ie_bucket_id
)
select * from azure_data_invoice_staged
