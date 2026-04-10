{{
    config(materialized="view")
}}

with base_data as (
    select
        date::date as date,
        cluster_hostname,
        merge_name,
        sum(total_rows) as total_rows
    from argus.daily_usage
    where cluster_hostname in (
        'ams.trafficpeak.live', 'iad.trafficpeak.live',
        'maa1.trafficpeak.live', 'ord.trafficpeak.live',
        'lax.trafficpeak.live', 'sg-sin-2.trafficpeak.live',
        'osa.trafficpeak.live', 'osa-2.trafficpeak.live',
        'sea.trafficpeak.live', 'sea-2.trafficpeak.live',
        'lon.trafficpeak.live', 'fra.trafficpeak.live',
        'in-bom.trafficpeak.live', 'in-bom-2.trafficpeak.live'
    )
    and date >= '2025-01-01'
    group by 1, 2, 3
), cluster_daily_total as (
    select
        date,
        cluster_hostname,
        sum(total_rows) as cluster_total_rows
    from base_data
    group by 1, 2
), cluster_project_total as (
    select
        b.*,
        c.cluster_total_rows,
        round((b.total_rows / nullif(c.cluster_total_rows, 0))::numeric, 6) as pct_usage
    from base_data b
    left join cluster_daily_total c
        on b.date = c.date
        and b.cluster_hostname = c.cluster_hostname
)
select * from cluster_project_total
