# stg_linode_instance_billing_daily

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | incremental |

## Depends On

- `stg_linode_instance_billing`

## Columns

| Column | Type | Description |
|---|---|---|
| `daily_billing_id` | text |  |
| `cluster_id` | varchar |  |
| `cluster_label` | varchar |  |
| `cluster_region` | varchar |  |
| `linode_id` | varchar |  |
| `linode_label` | varchar |  |
| `linode_type_label` | varchar |  |
| `cloud_account` | varchar |  |
| `invoice_month` | date |  |
| `reporting_date` | date |  |
| `active_days_in_month` | bigint |  |
| `total_amount` | float |  |
| `hdx_amount` | float |  |
| `daily_total_amount` | numeric |  |
| `daily_hdx_amount` | numeric |  |
