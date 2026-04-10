# stg_linode_instance_billing

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | table |

## Depends On

- `dim_month`
- `stg_linode_instance_with_shutdown`

## Columns

| Column | Type | Description |
|---|---|---|
| `cluster_id` | varchar |  |
| `cluster_created_date` | timestamp |  |
| `cluster_label` | varchar |  |
| `cluster_region` | varchar |  |
| `linode_id` | varchar |  |
| `linode_created_date` | timestamp |  |
| `linode_label` | varchar |  |
| `linode_final_status` | varchar |  |
| `linode_type_label` | varchar |  |
| `linode_type_list_price` | float |  |
| `linode_type_hdx_price` | float |  |
| `linode_type_monthly_list_price` | float |  |
| `linode_shutdown_date` | timestamp |  |
| `linode_created_month` | date |  |
| `linode_shutdown_month` | date |  |
| `invoice_month` | date |  |
| `billing_interval_start_timestamp` | timestamp |  |
| `billing_interval_end_timestamp` | timestamp |  |
| `months_hours` | numeric |  |
| `billable_hours` | numeric |  |
| `total_amount` | float |  |
| `hdx_amount` | float |  |
