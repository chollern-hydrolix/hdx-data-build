# mart_account_health_contract

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | table |

## Depends On

- `dim_account`
- `fct_contract`
- `fct_contract_daily_usage`
- `fct_contract_daily_usage`

## Columns

| Column | Type | Description |
|---|---|---|
| `status` | varchar |  |
| `contract_number` | varchar |  |
| `contract_id` | varchar |  |
| `replaced_by_new_contract` | boolean |  |
| `channel` | varchar |  |
| `region` | varchar |  |
| `country` | varchar |  |
| `type_calculated` | varchar |  |
| `mrr_gross` | float |  |
| `mrr_net` | float |  |
| `account_name` | varchar |  |
| `hydrolix_product` | varchar |  |
| `commit_amount` | float |  |
| `commit_type` | varchar |  |
| `commit_normalized` | float |  |
| `contract_start_date` | date |  |
| `contract_end_date` | date |  |
| `contract_start_month` | date |  |
| `contract_end_month` | date |  |
| `contract_short_id` | text |  |
| `usage_last_7_days` | float |  |
| `usage_days_last_7_days` | float |  |
| `avg_daily_gbs_last_7_days` | float |  |
| `max_qpm_last_7_days` | float |  |
| `pct_of_commit_last_7_days` | float |  |
| `pct_of_commit_group` | text |  |
| `max_qpm_group` | text |  |
