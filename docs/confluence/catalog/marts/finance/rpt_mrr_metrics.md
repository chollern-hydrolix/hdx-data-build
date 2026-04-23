# rpt_mrr_metrics

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | table |

## Depends On

- `fct_expiration_risk_contract`
- `stg_rpt_mrr_metrics`

## Columns

| Column | Type | Description |
|---|---|---|
| `account_name` | text |  |
| `type` | text |  |
| `type_calculated` | text |  |
| `type_reporting` | text |  |
| `channel` | text |  |
| `region` | text |  |
| `country` | text |  |
| `hydrolix_product` | text |  |
| `activated_effective_month` | timestamp |  |
| `churn_month` | timestamp |  |
| `reporting_month` | timestamp |  |
| `account_id` | varchar |  |
| `contract_id` | varchar |  |
| `contract_number` | text |  |
| `metric` | text |  |
| `value` | float |  |
| `label` | text |  |
| `label_with_sort` | text |  |
