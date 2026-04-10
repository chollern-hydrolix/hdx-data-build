# fct_usage__deployment_daily

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | table |

## Depends On

- `fct_crm__contract_deployment_history`
- `fct_crm__deployment`
- `fct_usage__project_daily`

## Columns

| Column | Type | Description |
|---|---|---|
| `salesforce_id` | character varying(18) |  |
| `deployment_id` | text |  |
| `account_id` | text |  |
| `reporting_date` | date |  |
| `total_rows` | float |  |
| `total_bytes` | float |  |
| `max_qpm` | float |  |
| `total_queries` | float |  |
| `contract_id` | character varying(18) |  |
