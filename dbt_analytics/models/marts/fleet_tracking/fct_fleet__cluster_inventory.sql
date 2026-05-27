{{ config(materialized="table") }}

with linode_clusters as (
    select
        label as cluster_name,
        'linode' as cloud_provider,
        cloud_account as provider_account,
        region,
        taskengine_last_sync_ts as last_sync_ts
    from linode.cluster
), eks_clusters as (
    select
        cluster_name,
        'aws' as cloud_provider,
        coalesce(account_name, account_id) as provider_account,
        region,
        taskengine_last_sync_ts as last_sync_ts
    from aws.eks_cluster
), cluster_union as (
    select * from linode_clusters
        union all
    select * from eks_clusters
)
select * from cluster_union
