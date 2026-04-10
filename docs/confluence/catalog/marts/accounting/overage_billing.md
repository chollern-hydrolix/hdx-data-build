# overage_billing

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | table |

## Depends On

- `dim_crm__account`
- `dim_month`
- `fct_crm__contract`
- `fct_crm__deployment`

## Columns

| Column | Type | Description |
|---|---|---|
| `account_name` | text |  |
| `contract_number` | text |  |
| `akamai_account_id` | text |  |
| `akamai_contract_id` | text |  |
| `contract_start_date` | date |  |
| `contract_end_date` | date |  |
| `commit_amount` | numeric |  |
| `commit_type` | text |  |
| `overage_charges` | text |  |
| `overage_charges_number` | numeric |  |
| `overage_commit_normalized` | float |  |
| `cluster_project` | text |  |
| `deployment_id` | character varying(18) |  |
| `reporting_month` | date |  |
| `table_name` | varchar |  |
| `total_bytes` | float |  |
| `total_rows` | float |  |
| `usage_amount` | float |  |
| `commit_normalized` | float |  |
| `overage_balance` | float |  |
