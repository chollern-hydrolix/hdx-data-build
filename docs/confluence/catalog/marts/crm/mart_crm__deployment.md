# mart_crm__deployment

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | table |

## Depends On

- `dim_crm__account`
- `fct_crm__contract`
- `fct_crm__contract_deployment_history`
- `fct_crm__deployment`

## Columns

| Column | Type | Description |
|---|---|---|
| `account_name` | text |  |
| `deployment_id` | text |  |
| `contract_number` | text |  |
| `contract_start_date` | date |  |
| `contract_end_date` | date |  |
| `reporting_start_date` | date |  |
| `reporting_end_date` | date |  |
| `tb_per_month_standard` | numeric |  |
| `tb_per_month_premium` | numeric |  |
| `standard_overages_per_gb` | numeric |  |
| `premium_overages_per_gb` | numeric |  |
| `raw_log_retention_standard` | numeric |  |
| `summary_log_retention_standard` | numeric |  |
| `commit_amount` | numeric |  |
| `commit_type` | text |  |
| `account_id` | character varying(18) |  |
| `salesforce_id` | character varying(18) |  |
| `contract_id` | character varying(18) |  |
