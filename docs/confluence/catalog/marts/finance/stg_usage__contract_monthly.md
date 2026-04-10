# stg_usage__contract_monthly

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | table |

## Depends On

- `dim_day`
- `fct_crm__contract`
- `fct_usage__deployment_daily`

## Columns

| Column | Type | Description |
|---|---|---|
| `contract_id` | character varying(18) |  |
| `reporting_month` | date |  |
| `total_bytes` | float |  |
| `total_rows` | float |  |
| `max_qpm` | float |  |
| `total_queries` | float |  |
| `cumulative_bytes` | float |  |
| `cumulative_rows` | float |  |
| `cumulative_max_qpm` | float |  |
