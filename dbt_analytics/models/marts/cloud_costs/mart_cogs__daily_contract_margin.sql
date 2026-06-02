{{
    config(
        materialized='incremental',
        unique_key='margin_daily_id',
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['reporting_date']},
            {'columns': ['deployment_sfid']},
            {'columns': ['contract_id']}
        ]
    )
}}

with days as (
    select day_date as reporting_date
    from {{ ref('dim_day') }}
    where day_date between '2025-01-01' and current_date
    {% if is_incremental() %}
      and day_date >= (select date_trunc('month', max(reporting_date))::date - interval '1 month' from {{ this }})
    {% endif %}
), contract_dimensions as (
    select
        c.contract_id,
        c.contract_number,
        c.contract_start_date,
        c.contract_end_date,
        c.status as contract_status,
        c.region,
        c.hydrolix_product,
        c.commit_type,
        c.commit_amount,
        cdh.salesforce_deployment_id as deployment_sfid,
        d.deployment_id as deployment_ulid,
        a.account_id,
        a.account_name
    from {{ ref('fct_crm__contract') }} c
    left join {{ ref('fct_crm__contract_deployment_history') }} cdh on c.contract_id = cdh.contract_id
    left join {{ ref('fct_crm__deployment') }} d on cdh.salesforce_deployment_id = d.salesforce_id
    left join {{ ref('dim_crm__account') }} a on c.account_id = a.account_id
), contract_months as (
    -- MRR-active months: ensures 0% variance from mart_mrr_contracts
    select distinct
        contract_id,
        reporting_month
    from {{ ref('mart_mrr_contracts') }}
    where reporting_month between '2025-01-01' and current_date
    {% if is_incremental() %}
      and reporting_month >= (select date_trunc('month', max(reporting_date))::date - interval '1 month' from {{ this }})
    {% endif %}
    union
    -- CDH-active months: ensures cost-eligible months still appear even if no MRR yet
    select distinct
        cdh.contract_id,
        dm.month_date as reporting_month
    from {{ ref('fct_crm__contract_deployment_history') }} cdh
    inner join {{ ref('dim_month') }} dm
        on dm.month_date >= date_trunc('month', cdh.reporting_start_date)::date
       and dm.month_date <= date_trunc('month', cdh.reporting_end_date)::date
    where dm.month_date between '2025-01-01' and current_date
    {% if is_incremental() %}
      and dm.month_date >= (select date_trunc('month', max(reporting_date))::date - interval '1 month' from {{ this }})
    {% endif %}
), contracts_with_days as (
    select
        d.reporting_date,
        cm.reporting_month,
        extract(day from (cm.reporting_month + interval '1 month' - interval '1 day'))::int as days_in_month,
        cd.account_name,
        cd.region,
        cd.contract_start_date,
        cd.contract_end_date,
        cd.contract_number,
        cd.contract_status,
        cd.hydrolix_product,
        cd.commit_amount,
        cd.commit_type,
        cd.deployment_sfid,
        cd.deployment_ulid,
        cm.contract_id,
        cd.account_id
    from contract_months cm
    inner join days d
        on date_trunc('month', d.reporting_date)::date = cm.reporting_month
    left join contract_dimensions cd on cm.contract_id = cd.contract_id
), daily_linode_estimate as (
    select
        b.reporting_date,
        m.deployment_sfid,
        sum(b.daily_total_amount * m.pro_rated_pct) as estimated_daily_linode_cost,
        sum(b.daily_hdx_amount * m.pro_rated_pct) as estimated_daily_linode_hdx_cost
    from {{ ref('stg_linode_instance_billing_daily') }} b
    inner join {{ ref('dim_cluster_to_deployment') }} m
        on m.allocation_month = b.invoice_month
       and m.cluster_hostname = concat(b.cluster_label, '.trafficpeak.live')
    {% if is_incremental() %}
    where b.reporting_date >= (select date_trunc('month', max(reporting_date))::date - interval '1 month' from {{ this }})
    {% endif %}
    group by 1, 2
), monthly_invoiced_linode as (
    select
        invoice_month,
        deployment_sfid,
        sum(total_linode_cost) as monthly_invoiced_linode_cost,
        sum(hdx_linode_cost) as monthly_invoiced_linode_hdx_cost,
        sum(premium_discount_linode_cost) as monthly_invoiced_premium_discount_linode_cost
    from {{ ref('fct_cogs__akamai_deployment_cost') }}
    group by 1, 2
), monthly_azure as (
    select
        invoice_month,
        deployment_sfid,
        sum(azure_cost) as monthly_azure_bucket_cost
    from {{ ref('fct_cogs__azure_bucket_cost') }}
    group by 1, 2
), monthly_mrr as (
    select
        reporting_month,
        contract_id,
        ending_mrr_gross as monthly_mrr
    from {{ ref('mart_mrr_contracts') }}
), daily_contract_usage as (
    select
        contract_id,
        reporting_date,
        sum(total_bytes) as total_bytes,
        sum(total_rows) as total_rows,
        max(max_qpm) as max_qpm
    from {{ ref('fct_usage__deployment_daily') }}
    {% if is_incremental() %}
    where reporting_date >= (select date_trunc('month', max(reporting_date))::date - interval '1 month' from {{ this }})
    {% endif %}
    group by 1, 2
), deployment_cluster_hostnames as (
    select
        deployment_sfid,
        allocation_month,
        string_agg(cluster_hostname, ', ' order by cluster_hostname) as cluster_hostnames
    from {{ ref('dim_cluster_to_deployment') }}
    group by 1, 2
), deployment_month_contract_count as (
    select
        deployment_sfid,
        reporting_month,
        count(distinct contract_id) as contracts_for_deployment_month
    from contracts_with_days
    where deployment_sfid is not null
    group by 1, 2
), joined as (
    select
        c.reporting_date,
        c.reporting_month,
        c.days_in_month,
        c.account_id,
        c.account_name,
        c.region,
        c.hydrolix_product,
        c.commit_type,
        c.commit_amount,
        c.contract_id,
        c.contract_number,
        c.contract_start_date,
        c.contract_end_date,
        c.contract_status,
        c.deployment_sfid,
        c.deployment_ulid,
        coalesce(lin.estimated_daily_linode_cost, 0) / coalesce(dmc.contracts_for_deployment_month, 1)::numeric as estimated_daily_linode_cost,
        coalesce(lin.estimated_daily_linode_hdx_cost, 0) / coalesce(dmc.contracts_for_deployment_month, 1)::numeric as estimated_daily_linode_hdx_cost,
        akm.monthly_invoiced_linode_cost,
        akm.monthly_invoiced_linode_hdx_cost,
        akm.monthly_invoiced_premium_discount_linode_cost,
        akm.monthly_invoiced_linode_cost::numeric / c.days_in_month / coalesce(dmc.contracts_for_deployment_month, 1)::numeric as invoiced_daily_linode_cost_prorated,
        akm.monthly_invoiced_linode_hdx_cost::numeric / c.days_in_month / coalesce(dmc.contracts_for_deployment_month, 1)::numeric as invoiced_daily_linode_hdx_cost_prorated,
        akm.monthly_invoiced_premium_discount_linode_cost::numeric / c.days_in_month / coalesce(dmc.contracts_for_deployment_month, 1)::numeric as invoiced_daily_premium_discount_linode_cost_prorated,
        coalesce(azr.monthly_azure_bucket_cost, 0) as monthly_azure_bucket_cost,
        coalesce(azr.monthly_azure_bucket_cost, 0)::numeric / c.days_in_month / coalesce(dmc.contracts_for_deployment_month, 1)::numeric as daily_azure_bucket_cost_prorated,
        mrr.monthly_mrr as monthly_ending_mrr,
        mrr.monthly_mrr::numeric / c.days_in_month as daily_mrr_prorated,
        du.total_bytes,
        du.total_bytes / (1000^3) as total_gb,
        du.total_bytes / (1024^3) as total_gib,
        du.total_bytes / (1000^4) as total_tb,
        du.total_bytes / (1024^4) as total_tib,
        du.total_rows,
        du.max_qpm,
        dch.cluster_hostnames
    from contracts_with_days c
    left join daily_linode_estimate lin
        on c.reporting_date = lin.reporting_date
       and c.deployment_sfid = lin.deployment_sfid
    left join monthly_invoiced_linode akm
        on c.reporting_month = akm.invoice_month
       and c.deployment_sfid = akm.deployment_sfid
    left join monthly_azure azr
        on c.reporting_month = azr.invoice_month
       and c.deployment_sfid = azr.deployment_sfid
    left join monthly_mrr mrr
        on c.reporting_month = mrr.reporting_month
       and c.contract_id = mrr.contract_id
    left join daily_contract_usage du
        on c.contract_id = du.contract_id
       and c.reporting_date = du.reporting_date
    left join deployment_month_contract_count dmc
        on c.deployment_sfid = dmc.deployment_sfid
       and c.reporting_month = dmc.reporting_month
    left join deployment_cluster_hostnames dch
        on c.deployment_sfid = dch.deployment_sfid
       and c.reporting_month = dch.allocation_month
), contracts_keys as (
    select distinct deployment_sfid, reporting_date, reporting_month
    from contracts_with_days
), linode_estimate_upstream_daily as (
    select
        b.reporting_date,
        sum(b.daily_total_amount) as upstream_total,
        sum(b.daily_hdx_amount) as upstream_hdx
    from {{ ref('stg_linode_instance_billing_daily') }} b
    {% if is_incremental() %}
    where b.reporting_date >= (select date_trunc('month', max(reporting_date))::date - interval '1 month' from {{ this }})
    {% endif %}
    group by 1
), linode_estimate_in_mart_daily as (
    select
        b.reporting_date,
        sum(b.estimated_daily_linode_cost) as in_mart_total,
        sum(b.estimated_daily_linode_hdx_cost) as in_mart_hdx
    from daily_linode_estimate b
    inner join contracts_keys c
        on c.deployment_sfid = b.deployment_sfid
       and c.reporting_date = b.reporting_date
    group by 1
), linode_invoiced_upstream_monthly as (
    select
        invoice_month,
        sum(total_linode_cost) as upstream_total,
        sum(hdx_linode_cost) as upstream_hdx,
        sum(premium_discount_linode_cost) as upstream_pdc
    from {{ ref('fct_cogs__akamai_deployment_cost') }}
    {% if is_incremental() %}
    where invoice_month >= (select date_trunc('month', max(reporting_date))::date - interval '1 month' from {{ this }})
    {% endif %}
    group by 1
), linode_invoiced_in_mart_monthly as (
    select
        m.invoice_month as reporting_month,
        sum(m.monthly_invoiced_linode_cost) as in_mart_total,
        sum(m.monthly_invoiced_linode_hdx_cost) as in_mart_hdx,
        sum(m.monthly_invoiced_premium_discount_linode_cost) as in_mart_pdc
    from monthly_invoiced_linode m
    inner join (select distinct deployment_sfid, reporting_month from contracts_keys) c
        on c.deployment_sfid = m.deployment_sfid
       and c.reporting_month = m.invoice_month
    group by 1
), azure_upstream_monthly as (
    select
        invoice_month,
        sum(azure_cost) as upstream_total
    from {{ ref('fct_cogs__azure_bucket_cost') }}
    {% if is_incremental() %}
    where invoice_month >= (select date_trunc('month', max(reporting_date))::date - interval '1 month' from {{ this }})
    {% endif %}
    group by 1
), azure_in_mart_monthly as (
    select
        m.invoice_month as reporting_month,
        sum(m.monthly_azure_bucket_cost) as in_mart_total
    from monthly_azure m
    inner join (select distinct deployment_sfid, reporting_month from contracts_keys) c
        on c.deployment_sfid = m.deployment_sfid
       and c.reporting_month = m.invoice_month
    group by 1
), unallocated_per_day as (
    select
        d.reporting_date,
        date_trunc('month', d.reporting_date)::date as reporting_month,
        extract(day from (date_trunc('month', d.reporting_date) + interval '1 month' - interval '1 day'))::int as days_in_month,
        coalesce(le_up.upstream_total, 0) - coalesce(le_im.in_mart_total, 0) as unalloc_est_linode,
        coalesce(le_up.upstream_hdx, 0) - coalesce(le_im.in_mart_hdx, 0) as unalloc_est_linode_hdx,
        coalesce(li_up.upstream_total, 0) - coalesce(li_im.in_mart_total, 0) as unalloc_inv_linode_monthly,
        coalesce(li_up.upstream_hdx, 0) - coalesce(li_im.in_mart_hdx, 0) as unalloc_inv_linode_hdx_monthly,
        coalesce(li_up.upstream_pdc, 0) - coalesce(li_im.in_mart_pdc, 0) as unalloc_inv_pdc_monthly,
        coalesce(az_up.upstream_total, 0) - coalesce(az_im.in_mart_total, 0) as unalloc_azure_monthly
    from days d
    left join linode_estimate_upstream_daily le_up
        on le_up.reporting_date = d.reporting_date
    left join linode_estimate_in_mart_daily le_im
        on le_im.reporting_date = d.reporting_date
    left join linode_invoiced_upstream_monthly li_up
        on li_up.invoice_month = date_trunc('month', d.reporting_date)::date
    left join linode_invoiced_in_mart_monthly li_im
        on li_im.reporting_month = date_trunc('month', d.reporting_date)::date
    left join azure_upstream_monthly az_up
        on az_up.invoice_month = date_trunc('month', d.reporting_date)::date
    left join azure_in_mart_monthly az_im
        on az_im.reporting_month = date_trunc('month', d.reporting_date)::date
), unallocated_rows as (
    select
        reporting_date,
        reporting_month,
        days_in_month,
        'UNALLOCATED' as account_id,
        'UNALLOCATED' as account_name,
        null::text as region,
        null::text as hydrolix_product,
        null::text as commit_type,
        null::numeric as commit_amount,
        'UNALLOCATED' as contract_id,
        null::text as contract_number,
        null::date as contract_start_date,
        null::date as contract_end_date,
        null::text as contract_status,
        'UNALLOCATED' as deployment_sfid,
        null::text as deployment_ulid,
        unalloc_est_linode as estimated_daily_linode_cost,
        unalloc_est_linode_hdx as estimated_daily_linode_hdx_cost,
        unalloc_inv_linode_monthly as monthly_invoiced_linode_cost,
        unalloc_inv_linode_hdx_monthly as monthly_invoiced_linode_hdx_cost,
        unalloc_inv_pdc_monthly as monthly_invoiced_premium_discount_linode_cost,
        unalloc_inv_linode_monthly::numeric / days_in_month as invoiced_daily_linode_cost_prorated,
        unalloc_inv_linode_hdx_monthly::numeric / days_in_month as invoiced_daily_linode_hdx_cost_prorated,
        unalloc_inv_pdc_monthly::numeric / days_in_month as invoiced_daily_premium_discount_linode_cost_prorated,
        unalloc_azure_monthly as monthly_azure_bucket_cost,
        unalloc_azure_monthly::numeric / days_in_month as daily_azure_bucket_cost_prorated,
        null::numeric as monthly_ending_mrr,
        null::numeric as daily_mrr_prorated,
        null::numeric as total_bytes,
        null::numeric as total_gb,
        null::numeric as total_gib,
        null::numeric as total_tb,
        null::numeric as total_tib,
        null::numeric as total_rows,
        null::numeric as max_qpm,
        null::text as cluster_hostnames
    from unallocated_per_day
    where coalesce(unalloc_est_linode, 0) != 0
       or coalesce(unalloc_inv_linode_monthly, 0) != 0
       or coalesce(unalloc_azure_monthly, 0) != 0
), all_rows as (
    select * from joined
    union all
    select * from unallocated_rows
), rows_with_margin as (
    select
        *,
        daily_mrr_prorated - estimated_daily_linode_hdx_cost - monthly_azure_bucket_cost as estimated_daily_margin
    from all_rows
)
select
    {{ dbt_utils.generate_surrogate_key([
        'contract_id',
        'deployment_sfid',
        'reporting_date'
    ]) }} as margin_daily_id,
    *
from rows_with_margin
