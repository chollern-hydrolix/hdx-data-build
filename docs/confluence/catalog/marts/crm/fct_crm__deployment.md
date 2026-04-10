# fct_crm__deployment

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | table |

## Depends On

- `stg_crm__child_contracts`

## Columns

| Column | Type | Description |
|---|---|---|
| `deployment_id` | text |  |
| `account_id` | text |  |
| `contract_id` | text |  |
| `opportunity_id` | text |  |
| `stage_name` | text |  |
| `created_date` | timestamp |  |
| `last_modified_date` | timestamp |  |
| `salesforce_id` | character varying(18) | Unique ID of the Deployment |
| `salesforce_short_id` | text |  |
| `system_modstamp` | timestamp |  |
