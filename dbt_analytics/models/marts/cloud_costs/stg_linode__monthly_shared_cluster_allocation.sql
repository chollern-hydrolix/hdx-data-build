{{config(mterialized="table")}}

with shared_clusters as (
    select
        name as cluster_hostname
    from raw_salesforce.ie_cluster__c
    where is_deleted is False
    and hdx_shared_cluster__c is True
    and name ilike '%trafficpeak%'
), monthly_project_usage_data as (
    select
        date_trunc('month', u.date)::date as month,
        u.cluster_hostname,
        u.project_name,
        concat(u.cluster_hostname, '-', u.project_name) as cluster_project_name,
        sum(u.total_bytes) as total_project_bytes
    from argus.daily_usage_with_table u
    inner join shared_clusters c on u.cluster_hostname = c.cluster_hostname
    where project_name != 'hydro'
    group by 1, 2, 3, 4
), monthly_cluster_usage_data as (
    select
        u.month,
        u.cluster_hostname,
        sum(total_project_bytes) as total_cluster_bytes
    from monthly_project_usage_data u
    group by 1, 2
), monthly_usage_allocation as (
    select
        p.month,
        p.cluster_hostname,
        p.project_name,
        p.cluster_project_name,
        p.total_project_bytes,
        c.total_cluster_bytes,
        (p.total_project_bytes / nullif(c.total_cluster_bytes, 0)) as monthly_pro_rated_pct
    from monthly_project_usage_data p
    left join monthly_cluster_usage_data c
        on p.month = c.month
        and p.cluster_hostname = c.cluster_hostname
)
select * from monthly_usage_allocation
