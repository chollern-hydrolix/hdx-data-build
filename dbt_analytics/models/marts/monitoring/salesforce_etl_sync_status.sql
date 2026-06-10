{{ config(materialized='view') }}

/*
===============================================================================
Model:
    salesforce_etl_sync_status

Grain:
    One row per tracked Salesforce object in internal.salesforce_object

Description:
    Joins every tracked Salesforce object to the max taskengine_last_sync_ts
    from its corresponding raw_salesforce table. Objects with no matching table
    (not yet synced) surface with null last_sync_ts and sync_status = 'never_synced'.

    The raw_salesforce_sync_status() macro dynamically discovers all raw_salesforce
    tables that have a taskengine_last_sync_ts column at runtime, so new objects
    are picked up automatically without model changes.

    Joins on salesforce_name_to_table(so.name), which applies the same camelCase
    -> snake_case conversion as the ETL pipeline to match raw_salesforce table names.
===============================================================================
*/

with salesforce_objects as (
    select
        name,
        label,
        key_prefix,
        full_sync_lookback_days
    from {{ source('internal', 'salesforce_object') }}
), sync_times as (
    {{ raw_salesforce_sync_status() }}
)
select
    so.*,
    ss.raw_table_name,
    ss.last_sync_ts,
    current_timestamp::timestamptz - ss.last_sync_ts          as time_since_last_sync,
    round(
        extract(epoch from (current_timestamp::timestamptz - ss.last_sync_ts))
        / 3600.0,
        2
    )                                                          as hours_since_last_sync,
    case
        when ss.last_sync_ts is null then 'never_synced'
        when current_timestamp - ss.last_sync_ts > interval '24 hours' then 'stale'
        when current_timestamp - ss.last_sync_ts > interval '6 hours' then 'delayed'
        else 'fresh'
    end                                                        as sync_status
from salesforce_objects so
left join sync_times ss
    on {{ salesforce_name_to_table('so.name') }} = ss.raw_table_name
