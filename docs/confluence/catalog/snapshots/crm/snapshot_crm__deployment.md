# snapshot_crm__deployment

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | snapshot |
| **Strategy** | timestamp |
| **Unique Key** | `salesforce_id` |
| **Updated At** | `system_modstamp` |

## Depends On

- `fct_crm__deployment`

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
| `dbt_scd_id` | text |  |
| `dbt_updated_at` | timestamp |  |
| `dbt_valid_from` | timestamp |  |
| `dbt_valid_to` | timestamp |  |
| `system_modstamp` | timestamp |  |
