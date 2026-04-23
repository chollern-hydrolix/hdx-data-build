# fct_active_contract

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | table |

## Columns

| Column | Type | Description |
|---|---|---|
| `account_name` | varchar |  |
| `contract_number` | varchar |  |
| `start_date` | date |  |
| `end_date` | date |  |
| `status` | varchar |  |
| `type` | varchar |  |
| `type_calculated` | varchar |  |
| `type_reporting` | varchar |  |
| `channel` | varchar |  |
| `region` | varchar |  |
| `country` | varchar |  |
| `hydrolix_product` | varchar |  |
| `mrr_gross` | float |  |
| `mrr_net` | float |  |
| `gmrr_gross` | float |  |
| `gmrr_net` | float |  |
| `lmrr_gross` | float |  |
| `lmrr_net` | float |  |
| `mrr_gross_expiration_risk` | float |  |
| `mrr_net_expiration_risk` | float |  |
| `activated_effective_month` | timestamp |  |
| `churn_month` | timestamp |  |
| `reporting_month` | timestamp |  |
| `start_month` | timestamptz |  |
| `end_month` | timestamptz |  |
| `expiration_month` | timestamp |  |
| `account_id` | varchar |  |
| `contract_id` | varchar |  |
| `is_expiration_risk` | boolean |  |
