{{config(materialized="table")}}

with ie_cluster_with_deployment as (
    select
        ie_c.name as cluster_hostname,
        coalesce(c.id, 'N/A') as contract_id,
        coalesce(c.original_contract_start__c, '2099-12-31'::date) as original_contract_start_date,
        ie_c.hdx_shared_cluster__c as is_shared_cluster
    from raw_salesforce.ie_cluster__c ie_c
    left join raw_salesforce.deployment__c d on ie_c.deployment__c = d.id
    left join raw_salesforce.contract c on d.contract__c = c.id
    where ie_c.is_deleted is False
), raw_akamai_invoice as (
    select
        invoice_name,
        date_trunc('month', invoice_publish_month - interval '1 month')::date as invoice_month,
        'Linode' as cloud_provider,
        cloud_account,
        case
            when cluster_label is not null then concat(cluster_label, '.trafficpeak.live')
            else 'N/A'
        end as cluster_hostname,
        total,
        premium_discount_total,
        hdx_total
    from analytics.fct_akm__invoice_item
    where total >= 0
), raw_akamai_invoice_with_salesforce_data as (
    select
        r.*,
        c.contract_id,
        c.original_contract_start_date,
        coalesce(c.is_shared_cluster, False) as is_shared_cluster
    from raw_akamai_invoice r
    left join ie_cluster_with_deployment c on r.cluster_hostname = c.cluster_hostname
), unique_ie_projects as (
    select
        distinct cluster_project__c,
                 deployment__c
    from raw_salesforce.ie_project__c
    where is_deleted is False
), shared_cluster_paid_poc_allocation as (
    select
        alc.month,
        alc.cluster_hostname,
        case
            when c.original_contract_start__c <= alc.month then 'PAID'
            else 'POC'
        end as cost_type,
        sum(monthly_pro_rated_pct) as pro_rated_pct
    from analytics.stg_linode__monthly_shared_cluster_allocation alc
    left join unique_ie_projects ie_p on alc.cluster_project_name = ie_p.cluster_project__c
    left join raw_salesforce.deployment__c d on ie_p.deployment__c = d.id
    left join raw_salesforce.contract c on d.contract__c = c.id
    where alc.month >= '2025-01-01'
    group by 1, 2, 3
), cluster_with_metadata as (
    select distinct on (ie_c.name)
        ie_c.name as ie_cluster_name,
        d.ulid__c as deployment_ulid,
        d.id as deployment_sfid,
        a.name as account_name,
        o.name as opportunity_name,
        c.contract_number as contract_number,
        o.stage_name as opportunity_stage_name,
        o.close_date as opportunity_close_date,
        o.id as opportunity_id,
        c.id as contract_id
    from raw_salesforce.ie_cluster__c ie_c
    left join raw_salesforce.deployment__c d on ie_c.deployment__c = d.id
    left join raw_salesforce.opportunity o on d.opportunity__c = o.id
    left join raw_salesforce.contract c on d.contract__c = c.id
    left join raw_salesforce.account a on o.account_id = a.id
    where ie_c.is_deleted is False
), stg_akamai_invoice as (
    select
        r.invoice_name,
        r.invoice_month,
        r.cloud_provider,
        r.cloud_account,
        r.total as linode_total,
        r.premium_discount_total as linode_premium_discount_total,
        r.hdx_total as linode_hdx_total,
        0 as enterprise_discount_total,
        0 as premium_discount_total,
        0 as poc_credit_total,
        0 as promotion_credit_total,
        0 as azure_cost,
        case
            when cloud_account = 'warner' then 'PAID'
            when cloud_account = 'trafficpeak-ops' then 'S&M'
            when cloud_account = 'dev' then 'R&D'
            when cloud_account = 'prod' then
                case
                    when r.cluster_hostname ilike '%test-%' or r.cluster_hostname ilike '%dev-%' then 'R&D'
                    when r.cluster_hostname != 'N/A' then
                        case
                            when r.is_shared_cluster then 'SHARED'
                            else
                                case
                                    when r.contract_id is null then 'POC'
                                    when r.original_contract_start_date > r.invoice_month then 'POC'
                                    when r.original_contract_start_date <= r.invoice_month then 'PAID'
                                    else 'INTERNAL'
                                end
                        end
                    else 'INTERNAL'
                end
            else 'INTERNAL'
        end as cost_type,
        r.cluster_hostname,
        cwm.deployment_ulid,
        cwm.deployment_sfid,
        cwm.account_name,
        cwm.opportunity_name,
        cwm.contract_number,
        cwm.opportunity_stage_name,
        cwm.opportunity_close_date,
        cwm.opportunity_id,
        cwm.contract_id
    from raw_akamai_invoice_with_salesforce_data r
    left join cluster_with_metadata cwm on r.cluster_hostname = cwm.ie_cluster_name
), stg_akamai_invoice_dedicated as (
    select
        invoice_name,
        invoice_month,
        cloud_provider,
        cloud_account,
        linode_total,
        linode_premium_discount_total,
        linode_hdx_total,
        enterprise_discount_total as invoice_enterprise_discount_total,
        premium_discount_total as invoice_premium_discount_total,
        poc_credit_total as invoice_poc_credit_total,
        promotion_credit_total as invoice_promotion_credit_total,
        azure_cost,
        cost_type,
        cluster_hostname,
        deployment_ulid,
        deployment_sfid,
        account_name,
        opportunity_name,
        contract_number,
        opportunity_stage_name,
        opportunity_close_date,
        opportunity_id,
        contract_id
    from stg_akamai_invoice
    where cost_type != 'SHARED'
), stg_akamai_invoice_shared as (
    select
        r.invoice_name,
        r.invoice_month,
        r.cloud_provider,
        r.cloud_account,
        r.linode_total * alc.pro_rated_pct as linode_total,
        r.linode_premium_discount_total * alc.pro_rated_pct as linode_premium_discount_total,
        r.linode_hdx_total * alc.pro_rated_pct as linode_hdx_total,
        r.enterprise_discount_total * alc.pro_rated_pct as invoice_enterprise_discount_total,
        r.premium_discount_total * alc.pro_rated_pct as invoice_premium_discount_total,
        r.poc_credit_total * alc.pro_rated_pct as invoice_poc_credit_total,
        r.promotion_credit_total * alc.pro_rated_pct as invoice_promotion_credit_total,
        r.azure_cost,
        alc.cost_type as cost_type,
        r.cluster_hostname,
        deployment_ulid,
        deployment_sfid,
        account_name,
        opportunity_name,
        contract_number,
        opportunity_stage_name,
        opportunity_close_date,
        opportunity_id,
        contract_id
    from stg_akamai_invoice r
    left join shared_cluster_paid_poc_allocation alc
        on r.cluster_hostname = alc.cluster_hostname
        and r.invoice_month = alc.month
    where r.cost_type = 'SHARED'
), stg_akamai_invoice_union as (
    select * from stg_akamai_invoice_dedicated
        union all
    select * from stg_akamai_invoice_shared
), raw_akamai_discount as (
    select
        *,
        case
            when label = 'Promotion Credits' then 'PROMOTION'
            when label ilike '%poc credit%' then 'POC'
            when label ilike '%premium%standard%' then 'PREMIUM'
            when label ilike '%discount%' or label ilike '%premium%discount%' then 'ENTERPRISE'
            else 'N/A'
        end as discount_type
    from analytics.fct_akm__invoice_item
    where total < 0
), stg_akamai_discount as (
    select
        invoice_name,
        case
            when discount_type = 'PROMOTION' then date_trunc('month', invoice_publish_month - interval '1 month')::date
            when discount_type = 'POC' then to_date(replace(replace(replace(label, ' ', ''), 'POCCredits', ''), 'POCCredit', ''), 'MonthYYYY')::date
            when discount_type = 'PREMIUM' then to_date(replace(replace(replace(label, ' ', ''), 'PremiumvsStandardCredit-', ''), 'PremiumVersusStandardCredit-', ''), 'MonthYYYY')::date
            when discount_type = 'ENTERPRISE' then to_date(replace(replace(replace(label, ' ', ''), 'Premium', ''), 'Discount-', ''), 'MonthYYYY')::date
            -- when discount_type = 'PROMOTION' then date_trunc('month', invoice_publish_month - interval '1 month')::date
            -- when discount_type = 'POC' then to_date(replace(replace(label, ' POC Credits', ''), ' POC Credit', ''), 'Month YYYY')::date
            -- when discount_type = 'PREMIUM' then to_date(replace(replace(label, 'Premium vs Standard Credit - ', ''), 'Premium Versus Standard Credit - ', ''), 'Month YYYY')::date
            -- when discount_type = 'ENTERPRISE' then to_date(replace(replace(label, 'Premium ', ''), 'Discount - ', ''), 'Month YYYY')::date
        end as invoice_month,
        'Linode' as cloud_provider,
        cloud_account,
        0 as total,
        0 as premium_discount_total,
        0 as hdx_total,
        case when discount_type = 'ENTERPRISE' then total else 0 end as invoice_enterprise_discount_total,
        case when discount_type = 'PREMIUM' then total else 0 end as invoice_premium_discount_total,
        case when discount_type = 'POC' then total else 0 end as invoice_poc_credit_total,
        case when discount_type = 'PROMOTION' then total else 0 end as invoice_promotion_credit_total,
        0 as azure_cost,
        'AKAMAI INVOICE' as cost_type,
        'N/A' as cluster_hostname,
        'N/A' as deployment_ulid,
        'N/A' as deployment_sfid,
        'N/A' as account_name,
        'N/A' as opportunity_name,
        'N/A' as contract_number,
        'N/A' as opportunity_stage_name,
        null::date as opportunity_close_date,
        'N/A' as opportunity_id,
        'N/A' as contract_id
    from raw_akamai_discount
    where discount_type != 'N/A'
), azure_data_invoice_staged as (
    select
        concat(to_char(a.invoice_month, 'Mon YYYY'), ' Azure Invoice') as invoice_name,
        a.invoice_month,
        'Azure' as cloud_provider,
        'azure-prod' as cloud_account,
        0 as linode_total,
        0 as linode_premium_discount_total,
        0 as linode_hdx_total,
        0 as invoice_enterprise_discount_total,
        0 as invoice_premium_discount_total,
        0 as invoice_poc_credit_total,
        0 as invoice_promotion_credit_total,
        a.azure_cost,
        a.cost_type,
        a.cluster_hostname,
        a.deployment_ulid,
        a.deployment_sfid,
        a.account_name,
        a.opportunity_name,
        a.contract_number,
        a.opportunity_stage_name,
        a.opportunity_close_date,
        a.opportunity_id,
        a.contract_id
    from {{ ref('fct_cogs__azure_bucket_cost') }} a
    left join {{ ref('fct_crm__contract') }} c on a.contract_id = c.contract_id
), invoice_data as (
    select * from stg_akamai_invoice_union
        union all
    select * from stg_akamai_discount
        union all
    select * from azure_data_invoice_staged
)
select * from invoice_data
