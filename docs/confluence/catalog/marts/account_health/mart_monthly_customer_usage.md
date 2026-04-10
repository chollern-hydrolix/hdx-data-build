# mart_monthly_customer_usage

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | table |

## Depends On

- `dim_month`

## Columns

| Column | Type | Description |
|---|---|---|
| `account_name` | text |  |
| `contract_number` | text |  |
| `opportunity_name` | text |  |
| `is_poc` | boolean |  |
| `opportunity_id` | text |  |
| `contract_id` | varchar |  |
| `account_id` | text |  |
| `deployment_id` | character varying(18) |  |
| `mrr` | numeric |  |
| `sales_region` | text |  |
| `hydrolix_product` | text |  |
| `pricing_calculator_version` | numeric |  |
| `created_date` | timestamp |  |
| `start_date` | date |  |
| `end_date` | date |  |
| `stage_name` | text |  |
| `poc_requested_date` | date |  |
| `poc_retirement_date` | date |  |
| `poc_status` | text |  |
| `poc_age_days` | interval |  |
| `tb_per_month_standard` | numeric |  |
| `commit_amount` | numeric |  |
| `commit_type` | text |  |
| `start_month` | date |  |
| `end_month` | date |  |
| `akamai_contract_id` | text |  |
| `akamai_account_id` | text |  |
| `contract_overrun` | boolean |  |
| `primary_industry` | text |  |
| `primary_sub_industry` | text |  |
| `sub_industry` | text |  |
| `country` | text |  |
| `reporting_month` | date |  |
| `total_bytes` | numeric |  |
| `total_rows` | numeric |  |
| `max_qpm` | numeric |  |
| `avg_daily_max_qpm` | numeric |  |
| `total_tib` | float |  |
| `total_tb` | float |  |
| `total_billion_rows` | numeric |  |
| `pct_of_commit` | float |  |
| `pct_of_commit_pro_rated` | float |  |
| `poc_flag` | text |  |
| `contract_flag` | text |  |
| `ingest_flag` | text |  |
| `query_flag` | text |  |
| `status` | text |  |
