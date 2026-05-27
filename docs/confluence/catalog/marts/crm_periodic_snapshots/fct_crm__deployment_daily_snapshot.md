# fct_crm__deployment_daily_snapshot

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | table |

## Depends On

- `dim_day`
- `snapshot_crm__deployment`
- `snapshot_crm__deployment`

## Columns

| Column | Type | Description |
|---|---|---|
| `deployment_id` | text |  |
| `account_id` | text |  |
| `contract_id` | text |  |
| `opportunity_id` | text |  |
| `stage_name` | text |  |
| `ie_name` | text |  |
| `is_shared` | boolean |  |
| `created_date` | timestamp |  |
| `last_modified_date` | timestamp |  |
| `salesforce_id` | character varying(18) |  |
| `salesforce_short_id` | text |  |
| `system_modstamp` | timestamp |  |
| `snapshot_date` | date |  |
