# mart_deployment_health

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | table |

## Depends On

- `dim_account`
- `fct_contract`
- `fct_deployment`

## Columns

| Column | Type | Description |
|---|---|---|
| `account_name` | varchar |  |
| `hydrolix_product` | varchar |  |
| `cluster_hostname` | varchar |  |
| `hdx_deployment_name` | varchar |  |
| `cluster_project_name_calculated` | varchar |  |
| `cluster_type_calculated` | varchar |  |
| `contract_number` | varchar |  |
| `poc` | boolean |  |
| `last_verified` | date |  |
| `deployment_id` | varchar |  |
| `opportunity_id` | varchar |  |
| `cluster_status` | text |  |
