# salesforce_etl_sync_status

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | view |

## Columns

| Column | Type | Description |
|---|---|---|
| `name` | varchar |  |
| `label` | varchar |  |
| `key_prefix` | varchar |  |
| `full_sync_lookback_days` | integer |  |
| `raw_table_name` | text |  |
| `last_sync_ts` | timestamp |  |
| `time_since_last_sync` | interval |  |
| `hours_since_last_sync` | numeric |  |
| `sync_status` | text |  |
