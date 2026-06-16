{{
    config(
        materialized='table',
        unique_key='invoice_item_id',
        indexes=[
            {'columns': ['cloud_account']},
            {'columns': ['invoice_publish_month']}
        ]
    )
}}

with linode_sf_hostname_map as (
    select distinct on (pod_namespace)
        pod_namespace,
        cluster_hostname as sf_cluster_hostname
    from argus.cluster_hostname_label_mapping
    where cluster_hostname != pod_namespace || '.trafficpeak.live'
    order by pod_namespace, most_recent_log desc nulls last
), invoice_items as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'i.id',
            'item.label',
            'item.from_dt',
            'item.to_dt',
            'item.total'
        ]) }} as invoice_item_id,
        i.label as invoice_name,
        i.date::date as invoice_publish_date,
        date_trunc('month', i.date)::date as invoice_publish_month,
        i.id as invoice_id,
        i.cloud_account,
        item.label,
        split_part(item.label, ' - ', 1) as resource_type,  -- regexp_replace(item.label, ' - .*$', '') as resource_type,
        substring(item.label from '-.*?\(([^)]*)\)') as resource_id,
        substring(item.label from 'lke([^-\s][^-]*)') as linode_cluster_id,
        item.from_dt,
        item.to_dt,
        item.total
    from {{source('linode', 'invoice_item')}} item
    left join {{source('linode', 'invoice')}} i on item.invoice_id = i.id
), distinct_volume_attachments as (
    select distinct on (e.entity_id)
        e.entity_id as linode_volume_id,
        c.id as cluster_id
    from {{source('linode', 'event')}} e
    left join {{source('linode', 'linode_instance')}} li on e.secondary_entity_id = li.id
    left join {{source('linode', 'cluster')}} c on li.lke_cluster_id = c.id
    where e.action = 'volume_attach'
      and e.entity_id is not null
      and e.secondary_entity_id is not null
    order by e.entity_id, e.created desc
), invoice_items_with_resources as (
    select
        items.*,
        coalesce(items.linode_cluster_id, lv_li.lke_cluster_id, c.id, dva.cluster_id) as cluster_id
    from invoice_items items
    left join {{source('linode', 'linode_volume')}} lv on items.resource_id = lv.id
    left join {{source('linode', 'linode_instance')}} lv_li on lv.linode_id = lv_li.id
    left join {{source('linode', 'cluster')}} c on items.resource_id = c.id
    left join distinct_volume_attachments dva on items.resource_id = dva.linode_volume_id
), invoice_items_with_cluster as (
    select
        items.*,
        c.label as cluster_label
    from invoice_items_with_resources items
    left join {{source('linode', 'cluster')}} c on items.cluster_id = c.id
), invoice_items_with_discount as (
    select
        items.invoice_item_id,
        items.invoice_name,
        items.invoice_publish_date,
        items.invoice_publish_month,
        items.cloud_account,
        items.cluster_label,
        items.cluster_id,
        items.invoice_id,
        items.label,
        items.resource_type,
        items.resource_id,
        items.from_dt,
        items.to_dt,
        items.total,
        case
            when items.resource_type ilike '%premium%' then total * 0.8324
            else total
        end as premium_discount_total
    from invoice_items_with_cluster items
), invoice_items_with_hdx_cost as (
    select
        items.*,
        items.premium_discount_total * 0.7 as hdx_total
    from invoice_items_with_discount items
)
select
    items.*,
    m.sf_cluster_hostname,
    coalesce(m.sf_cluster_hostname, items.cluster_label || '.trafficpeak.live') as cluster_hostname
from invoice_items_with_hdx_cost items
left join linode_sf_hostname_map m on items.cluster_label = m.pod_namespace
