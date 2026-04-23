# stg_contract_azure_usage

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | view |

## Depends On

- `fct_contract_deployment_history`
- `fct_deployment`

## Columns

| Column | Type | Description |
|---|---|---|
| `contract_id` | varchar |  |
| `azure_usage_date` | date |  |
| `azure_cost` | float |  |
