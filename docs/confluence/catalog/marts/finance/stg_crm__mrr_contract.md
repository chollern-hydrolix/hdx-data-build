# stg_crm__mrr_contract

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | view |

## Columns

| Column | Type | Description |
|---|---|---|
| `start_date` | date |  |
| `end_date` | date |  |
| `contract_term` | integer |  |
| `mrr_net` | numeric |  |
| `mrr_gross` | numeric |  |
| `gmrr_gross` | float |  |
| `gmrr_net` | float |  |
| `lmrr_gross` | numeric |  |
| `lmrr_net` | numeric |  |
| `arr` | numeric |  |
| `contract_number` | text |  |
| `tcv` | numeric |  |
| `churn_date_reporting` | date |  |
| `churn_date_reporting_month` | date |  |
| `churn_date_reporting_quarter` | text |  |
| `finance_reporting_date` | date |  |
| `type_calculated` | text |  |
| `channel` | text |  |
| `hydrolix_product` | text |  |
| `region` | text |  |
| `country` | text |  |
| `status` | text |  |
| `type` | text |  |
| `account_name` | text |  |
| `account_id` | character varying(18) |  |
| `contract_id` | character varying(18) |  |
| `activated_effective_date` | date |  |
| `activated_effective_month` | date |  |
| `start_month` | timestamptz |  |
| `end_month` | timestamptz |  |
| `churn_mrr_gross_2025` | numeric |  |
| `churn_mrr_net_2025` | numeric |  |
| `churn_mrr_net` | numeric |  |
| `previous_contract_start_month` | date |  |
| `previous_contract_end_month` | date |  |
| `previous_contract_id` | text |  |
| `replaced_by_new_contract` | boolean |  |
| `is_bridge_renewal` | boolean |  |
| `replaced_by_draft_contract` | boolean |  |
| `commit_amount` | numeric |  |
| `commit_type` | text |  |
| `type_reporting` | text |  |
| `fx_impact_mrr` | numeric |  |
| `fx_rate` | numeric |  |
| `customer_count_impact` | numeric |  |
| `churn_confirmed` | boolean |  |
| `churn_confirmed_date` | date |  |
| `opportunity_id` | text |  |
| `sold_by` | text |  |
| `contract_active` | boolean |  |
| `industry` | text |  |
| `primary_industry` | text |  |
| `primary_sub_industry` | text |  |
| `akamai_sales_rep` | text |  |
