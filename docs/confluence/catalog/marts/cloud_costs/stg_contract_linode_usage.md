# stg_contract_linode_usage

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | view |

## Depends On

- `fct_contract`
- `fct_contract_deployment_history`
- `fct_deployment`
- `stg_linode_instance_billing`
- `stg_shared_cluster_project_usage_estimate_pct`

## Columns

| Column | Type | Description |
|---|---|---|
| `contract_id` | varchar |  |
| `invoice_month` | date |  |
| `total_amount` | float |  |
