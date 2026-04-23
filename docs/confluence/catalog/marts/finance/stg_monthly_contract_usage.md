# stg_monthly_contract_usage

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | view |

## Depends On

- `fct_contract`
- `fct_contract_deployment_history`

## Columns

| Column | Type | Description |
|---|---|---|
| `contract_id` | varchar |  |
| `reporting_month` | date |  |
| `total_rows` | float |  |
| `total_bytes` | float |  |
