# fct_cogs__azure_bucket_cost

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | table |

## Depends On

- `int_cogs__ie_bucket_with_contract`

## Columns

| Column | Type | Description |
|---|---|---|
| `invoice_month` | date |  |
| `azure_cost` | float |  |
| `ie_bucket_id` | varchar |  |
| `bucket_name` | text |  |
| `cost_type` | text |  |
| `cluster_hostname` | text |  |
| `account_name` | text |  |
| `opportunity_name` | text |  |
| `contract_number` | text |  |
| `opportunity_stage_name` | text |  |
| `opportunity_close_date` | date |  |
| `deployment_ulid` | text |  |
| `deployment_sfid` | varchar |  |
| `opportunity_id` | text |  |
| `contract_id` | text |  |
