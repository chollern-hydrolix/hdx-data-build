# dim_cluster_to_deployment

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | table |

## Depends On

- `dim_month`
- `stg_linode__monthly_shared_cluster_allocation`

## Columns

| Column | Type | Description |
|---|---|---|
| `cluster_to_deployment_id` | text |  |
| `allocation_month` | date |  |
| `cluster_hostname` | text |  |
| `cluster_project_name` | text |  |
| `deployment_sfid` | text |  |
| `pro_rated_pct` | float |  |
| `allocation_type` | text |  |
