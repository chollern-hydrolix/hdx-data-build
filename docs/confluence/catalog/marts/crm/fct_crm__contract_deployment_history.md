# fct_crm__contract_deployment_history

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | table |

## Depends On

- `fct_crm__contract`
- `fct_crm__contract`
- `fct_crm__contract`
- `fct_crm__deployment`

## Columns

| Column | Type | Description |
|---|---|---|
| `contract_id` | character varying(18) | Unique ID of the Contract |
| `contract_number` | text |  |
| `previous_contract_id` | text |  |
| `salesforce_deployment_id` | character varying(18) |  |
| `deployment_id` | text |  |
| `contract_start_date` | date |  |
| `contract_end_date` | date |  |
| `reporting_start_date` | date |  |
| `reporting_end_date` | date |  |
| `reporting_start_month` | date |  |
| `reporting_end_month` | date |  |
