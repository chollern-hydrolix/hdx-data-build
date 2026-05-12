# mart_usage__infrastructure_monthly

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | table |

## Depends On

- `dim_crm__account`
- `dim_crm__account`
- `fct_crm__contract`
- `fct_crm__contract_deployment_history`
- `fct_crm__deployment`

## Columns

| Column | Type | Description |
|---|---|---|
| `cluster_project_table_name` | text |  |
| `reporting_month` | date |  |
| `account_name` | text |  |
| `contract_number` | text |  |
| `deployment_ulid` | text |  |
| `cluster_name` | text |  |
| `project_name` | text |  |
| `cluster_project_name` | text |  |
| `infrastructure_name` | text |  |
| `table_name` | text |  |
| `total_bytes` | float |  |
| `total_gb` | float |  |
| `total_gib` | float |  |
| `total_tb` | float |  |
| `total_tib` | float |  |
| `total_rows` | float |  |
| `commit_amount` | numeric |  |
| `commit_type` | text |  |
| `pct_of_commit` | float |  |
| `contract_start_date` | date |  |
| `contract_end_date` | date |  |
| `contract_status` | text |  |
| `deployment_status` | text |  |
| `cluster_status` | text |  |
| `project_status` | text |  |
| `is_hdx_shared_cluster` | boolean |  |
| `is_multi_tenant_cluster` | boolean |  |
| `is_multi_deployment_cluster` | boolean |  |
| `account_sfid` | varchar |  |
| `contract_sfid` | varchar |  |
| `deployment_sfid` | varchar |  |
| `ie_cluster_sfid` | varchar |  |
| `ie_project_sfid` | varchar |  |
| `ie_table_sfid` | varchar |  |
| `ie_cluster_last_sync_date` | date |  |
| `ie_project_last_sync_date` | date |  |
| `ie_table_last_sync_date` | date |  |
| `ie_cluster_last_usage_meter_sync_date` | date |  |
| `ie_project_last_usage_meter_sync_date` | date |  |
| `ie_table_last_usage_meter_sync_date` | date |  |
| `project_uuid` | text |  |
| `table_uuid` | text |  |
