# fct_usage__contract_billing_daily

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | incremental |

## Depends On

- `dim_crm__account`
- `fct_crm__contract`
- `fct_crm__contract_deployment_history`

## Columns

| Column | Type | Description |
|---|---|---|
| `contract_id` | character varying(18) |  |
| `deployment_sfid` | text |  |
| `billing_group` | text |  |
| `reporting_date` | date |  |
| `reporting_month` | date |  |
| `usage_type` | text |  |
| `account_id` | character varying(18) |  |
| `account_name` | text |  |
| `parent_account_name` | text |  |
| `contract_number` | text |  |
| `akamai_contract_id` | text |  |
| `contract_start_date` | date |  |
| `contract_end_date` | date |  |
| `contract_status` | text |  |
| `type_reporting` | text |  |
| `hydrolix_product` | text |  |
| `region` | text |  |
| `commit_type` | text |  |
| `commit_amount` | numeric |  |
| `mrr_gross` | numeric |  |
| `total_bytes` | float |  |
| `total_gb` | float |  |
| `total_gib` | float |  |
| `total_tb` | float |  |
| `total_tib` | float |  |
