# stg_daily_contract_linode_usage

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | view |

## Depends On

- `fct_contract`
- `fct_contract_deployment_history`
- `fct_deployment`
- `stg_daily_linode_resource_billing`
- `stg_daily_shared_cluster_project_usage_estimate_pct`

## Columns

| Column | Type | Description |
|---|---|---|
| `contract_id` | varchar |  |
| `reporting_date` | date |  |
| `total_amount` | float |  |
