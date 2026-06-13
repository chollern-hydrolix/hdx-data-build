{{ config(materialized="table") }}

/*

Joins Azure costs with Salesforce IE Buckets

*/

with invoiced_months as (
    select distinct billing_month
    from azure.monthly_cost_fact
), current_month_estimate as (
    -- Estimates current (uninvoiced) month costs from daily_usage_fact.
    -- Scoped to the current calendar month only to avoid scanning historical data
    -- and to prevent type-mismatch issues with the invoiced_months exclusion check.
    -- Splits resource_group-level spend into resource_name grain using historical
    -- cost ratios from stg_azure__resource_group_name_map.
    select
        date_trunc('month', current_date)::date  as billing_month,
        m.resource_name                          as bucket_name,
        sum(d.pre_tax_cost * m.cost_ratio)       as pre_tax_cost
    from azure.daily_usage_fact d
    inner join {{ ref('stg_azure__resource_group_name_map') }} m
        on d.resource_group = m.resource_group
    where date_trunc('month', d.usage_date)::date = date_trunc('month', current_date)::date
    group by m.resource_name
), azure_costs as (
    select
        billing_month,
        resource_name as bucket_name,
        sum(pre_tax_cost) as pre_tax_cost
    from azure.monthly_cost_fact
    group by 1, 2
    union all
    select billing_month, bucket_name, pre_tax_cost
    from current_month_estimate
    where billing_month not in (select billing_month from invoiced_months)
), ie_bucket as (
    select
        id as ie_bucket_id,
        storage_name__c as sf_storage_name,
        bucket_name__c as sf_bucket_name
    from raw_salesforce.ie_bucket__c
    where is_deleted is False
    and bucket_cloud__c ilike '%azure%'

-- Unpivot storage_name and bucket_name into a single lookup key.
-- DISTINCT on (lookup_key, ie_bucket_id) handles the case where the same bucket
-- matches via both fields without double-counting it.
), ie_bucket_unpivoted as (
    select sf_storage_name as lookup_key, ie_bucket_id
    from ie_bucket
    where sf_storage_name is not null

    union all

    select sf_bucket_name as lookup_key, ie_bucket_id
    from ie_bucket
    where sf_bucket_name is not null
), ie_bucket_all_matches as (
    select distinct
        lookup_key,
        ie_bucket_id
    from ie_bucket_unpivoted
-- One row per (lookup_key, deployment_sfid): cost is split by distinct deployments,
-- not raw IE Bucket record count. DISTINCT ON picks a representative ie_bucket_id
-- per deployment so the downstream join to int_cogs__ie_bucket_with_contract still works.
), ie_bucket_linked as (
    select distinct on (m.lookup_key, c.deployment_sfid)
        m.lookup_key,
        c.ie_bucket_id
    from ie_bucket_all_matches m
    inner join {{ ref('int_cogs__ie_bucket_with_contract') }} c on m.ie_bucket_id = c.ie_bucket_id
    where c.deployment_sfid is not null
    order by m.lookup_key, c.deployment_sfid, c.ie_bucket_id
), ie_bucket_match_counts as (
    select
        lookup_key,
        count(*) as match_count
    from ie_bucket_linked
    group by lookup_key
-- Divide each Azure bucket's cost evenly across distinct deployments.
-- coalesce(match_count, 1) preserves the full cost for buckets with no linked deployment.
), azure_allocated as (
    select
        ac.billing_month,
        ac.bucket_name,
        bm.ie_bucket_id,
        ac.pre_tax_cost / coalesce(mc.match_count, 1) as allocated_cost
    from azure_costs ac
    left join ie_bucket_linked bm on ac.bucket_name = bm.lookup_key
    left join ie_bucket_match_counts mc on ac.bucket_name = mc.lookup_key
), azure_joined as (
    select
        billing_month,
        ie_bucket_id,
        max(bucket_name) as bucket_name,
        sum(allocated_cost) as pre_tax_cost
    from azure_allocated
    group by 1, 2
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
        coalesce(ie_b.contract_id, 'N/A') as contract_id,
        ie_b.storage_name as ie_bucket_storage_name,
        ie_b.bucket_name as ie_bucket_bucket_name,
        a.bucket_name as azure_resource_name
    from azure_joined a
    left join {{ ref('int_cogs__ie_bucket_with_contract') }} ie_b on a.ie_bucket_id = ie_b.ie_bucket_id
)
select * from azure_data_invoice_staged
