# rpt_daily_linode_dev_costs

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | table |

## Depends On

- `stg_daily_linode_resource_billing`

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
| `is_premium` | boolean |  |
| `cloud_account` | varchar |  |
| `linode_created_date_only` | date |  |
| `linode_shutdown_date_only` | date |  |
| `reporting_date` | date |  |
| `reporting_month` | date |  |
| `current_month` | timestamptz |  |
| `is_current_month` | boolean |  |
| `billing_interval_start_timestamp` | timestamp |  |
| `billing_interval_end_timestamp` | timestamptz |  |
| `months_hours` | numeric |  |
| `billable_hours` | numeric |  |
| `total_amount` | float |  |
| `premium_discount_amount` | float |  |
| `hdx_amount` | float |  |
