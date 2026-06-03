# mart_contract_expiration

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | table |

## Depends On

- `dim_crm__account`
- `dim_month`
- `fct_crm__replacement_contract`
- `stg_crm__mrr_contract`
- `stg_crm__mrr_contract`
- `stg_crm__next_opportunity_by_account`
- `stg_usage__contract_monthly`

## Columns

| Column | Type | Description |
|---|---|---|
| `account_name` | text |  |
| `activated_effective_month` | date |  |
| `start_month` | date |  |
| `end_month` | date |  |
| `reporting_effective_month` | date |  |
| `reporting_end_month` | date |  |
| `mrr_gross` | numeric |  |
| `contract_id` | character varying(18) |  |
| `contract_number` | text |  |
| `replaced_by_new_contract` | boolean |  |
| `is_bridge_renewal` | boolean |  |
| `type_calculated` | text |  |
| `type` | text |  |
| `channel` | text |  |
| `region` | text |  |
| `country` | text |  |
| `hydrolix_product` | text |  |
| `type_reporting` | text |  |
| `account_id` | character varying(18) |  |
| `renewal_contract_id` | varchar |  |
| `renewal_contract_number` | text |  |
| `renewal_mrr_gross` | numeric |  |
| `renewal_type` | text |  |
| `is_event` | boolean |  |
| `replaced_by_draft_contract` | boolean |  |
| `churn_date_reporting_month` | date |  |
| `churn_confirmed` | boolean |  |
| `churn_confirmed_date` | date |  |
| `account_owner` | text |  |
| `sold_by` | text |  |
| `primary_industry` | text |  |
| `primary_sub_industry` | text |  |
| `commit_amount` | numeric |  |
| `commit_type` | text |  |
| `has_next_opportunity` | boolean |  |
| `next_opp_owner_name` | text |  |
| `next_opp_stage_name` | text |  |
| `next_opp_mrr_gross` | numeric |  |
| `next_opp_close_date` | date |  |
| `next_opp_probability` | numeric |  |
| `next_opp_type_reporting` | text |  |
| `reporting_month` | date |  |
| `is_up_for_renewal` | boolean |  |
| `is_renewed` | boolean |  |
| `renewal_category` | text |  |
| `total_mrr_expiring` | numeric |  |
| `flat_renewal_mrr` | numeric |  |
| `bridge_renewal_mrr` | numeric |  |
| `downgrade_renewal_mrr` | numeric |  |
| `upgrade_renewal_mrr` | numeric |  |
| `churn_mrr` | numeric |  |
| `total_mrr_outstanding` | numeric |  |
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
