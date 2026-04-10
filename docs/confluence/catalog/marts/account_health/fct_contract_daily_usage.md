# fct_contract_daily_usage

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | view |

## Columns

| Column | Type | Description |
|---|---|---|
| `id` | varchar |  |
| `timestamp` | timestamp |  |
| `date` | date |  |
| `account_name` | varchar |  |
| `contract_id` | varchar |  |
| `total_usage` | float |  |
| `usage_last_7_days` | float |  |
| `usage_days` | float |  |
| `usage_days_last_7_days` | float |  |
| `total_rows` | float |  |
| `total_bytes` | float |  |
| `bytes_last_7_days` | float |  |
| `max_qpm_last_7_days` | float |  |
| `hdx_pricing_last_7_days` | float |  |
| `avg_daily_gbs_last_7_days` | float |  |
| `avg_daily_hdx_pricing` | float |  |
