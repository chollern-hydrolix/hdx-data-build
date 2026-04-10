# mart_contract_expiration

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | table |

## Depends On

- `dim_account`
- `dim_month`
- `fct_replacement_contract`
- `stg_mrr_contracts`
- `stg_mrr_contracts`
- `stg_next_opportunity_by_account`
- `stg_usage__contract_monthly`

## Columns

| Column | Type | Description |
|---|---|---|
| `account_name` | varchar |  |
| `activated_effective_month` | date |  |
| `start_month` | date |  |
| `end_month` | date |  |
| `reporting_effective_month` | date |  |
| `reporting_end_month` | date |  |
| `mrr_gross` | float |  |
| `contract_id` | varchar |  |
| `contract_number` | varchar |  |
| `replaced_by_new_contract` | boolean |  |
| `is_bridge_renewal` | boolean |  |
| `type_calculated` | text |  |
| `type` | varchar |  |
| `channel` | varchar |  |
| `region` | varchar |  |
| `country` | varchar |  |
| `hydrolix_product` | varchar |  |
| `type_reporting` | varchar |  |
| `account_id` | varchar |  |
| `renewal_contract_id` | varchar |  |
| `renewal_contract_number` | varchar |  |
| `renewal_mrr_gross` | float |  |
| `renewal_type` | varchar |  |
| `is_event` | boolean |  |
| `replaced_by_draft_contract` | boolean |  |
| `churn_date_reporting_month` | date |  |
| `churn_confirmed` | boolean |  |
| `churn_confirmed_date` | date |  |
| `account_owner` | varchar |  |
| `sold_by` | varchar |  |
| `primary_industry` | varchar |  |
| `primary_sub_industry` | varchar |  |
| `commit_amount` | float |  |
| `commit_type` | varchar |  |
| `has_next_opportunity` | boolean |  |
| `next_opp_owner_name` | varchar |  |
| `next_opp_stage_name` | varchar |  |
| `next_opp_mrr_gross` | float |  |
| `next_opp_close_date` | date |  |
| `next_opp_probability` | float |  |
| `next_opp_type_reporting` | varchar |  |
| `reporting_month` | date |  |
| `is_up_for_renewal` | boolean |  |
| `is_renewed` | boolean |  |
| `renewal_category` | text |  |
| `total_mrr_expiring` | float |  |
| `flat_renewal_mrr` | float |  |
| `bridge_renewal_mrr` | float |  |
| `downgrade_renewal_mrr` | float |  |
| `upgrade_renewal_mrr` | float |  |
| `churn_mrr` | float |  |
| `total_mrr_outstanding` | float |  |
| `downgraded_mrr` | numeric |  |
| `upgraded_mrr` | numeric |  |
| `total_contracts_expiring` | integer |  |
| `flat_renewal_contracts` | integer |  |
| `bridge_renewal_contracts` | integer |  |
| `downgrade_renewal_contracts` | integer |  |
| `upgrade_renewal_contracts` | integer |  |
| `churn_contracts` | integer |  |
| `total_renewals_outstanding` | integer |  |
| `total_bytes` | float |  |
| `total_rows` | float |  |
| `total_usage_normalized` | float |  |
| `cumulative_bytes` | float |  |
| `cumulative_rows` | float |  |
| `cumulative_usage_normalized` | float |  |
| `max_qpm` | float |  |
| `total_queries` | float |  |
| `cumulative_max_qpm` | float |  |
| `should_remove` | boolean |  |
