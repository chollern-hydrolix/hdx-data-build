# mart_daily_table_usage

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | table |

## Depends On

- `dim_account`
- `dim_day`
- `fct_contract`
- `fct_deployment`

## Columns

| Column | Type | Description |
|---|---|---|
| `account_name` | varchar |  |
| `contract_number` | varchar |  |
| `akamai_account_id` | varchar |  |
| `akamai_contract_id` | varchar |  |
| `contract_start_date` | date |  |
| `contract_end_date` | date |  |
| `cluster_project` | text |  |
| `account_id` | varchar |  |
| `contract_id` | varchar |  |
| `deployment_id` | varchar |  |
| `reporting_date` | date |  |
| `reporting_month` | date |  |
| `total_days_in_month` | numeric |  |
| `cluster_hostname` | varchar |  |
| `project_name` | varchar |  |
| `table_name` | varchar |  |
| `total_bytes` | float |  |
| `total_rows` | float |  |
| `max_qpm` | float |  |
