# stg_mrr_contracts

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | table |

## Columns

| Column | Type | Description |
|---|---|---|
| `start_date` | date |  |
| `end_date` | date |  |
| `contract_term` | integer |  |
| `mrr_net` | float |  |
| `mrr_gross` | float |  |
| `gmrr_gross` | float |  |
| `gmrr_net` | float |  |
| `lmrr_gross` | float |  |
| `lmrr_net` | float |  |
| `arr` | float |  |
| `closed_date` | date |  |
| `contract_number` | varchar |  |
| `tcv` | float |  |
| `churn_date_reporting` | date |  |
| `churn_date_reporting_month` | date |  |
| `churn_date_reporting_quarter` | varchar |  |
| `finance_reporting_date` | date |  |
| `type_calculated` | text |  |
| `channel` | varchar |  |
| `hydrolix_product` | varchar |  |
| `region` | varchar |  |
| `country` | varchar |  |
| `closed_month` | date |  |
| `status` | varchar |  |
| `type` | varchar |  |
| `account_name` | varchar |  |
| `account_id` | varchar |  |
| `contract_id` | varchar |  |
| `activated_effective_date` | date |  |
| `activated_effective_month` | date |  |
| `start_month` | timestamptz |  |
| `end_month` | timestamptz |  |
| `churn_mrr_gross_2025` | float |  |
| `churn_mrr_net_2025` | float |  |
| `churn_mrr_net` | float |  |
| `previous_contract_start_month` | date |  |
| `previous_contract_end_month` | date |  |
| `previous_contract_id` | varchar |  |
| `replaced_by_new_contract` | boolean |  |
| `is_bridge_renewal` | boolean |  |
| `replaced_by_draft_contract` | boolean |  |
| `commit_amount` | float |  |
| `commit_type` | varchar |  |
| `type_reporting` | varchar |  |
| `fx_impact_mrr` | float |  |
| `fx_rate` | float |  |
| `customer_count_impact` | float |  |
| `churn_confirmed` | boolean |  |
| `churn_confirmed_date` | date |  |
| `opportunity_id` | varchar |  |
| `sold_by` | varchar |  |
| `contract_active` | boolean |  |
| `industry` | varchar |  |
| `primary_industry` | varchar |  |
| `primary_sub_industry` | varchar |  |
| `akamai_sales_rep` | varchar |  |
