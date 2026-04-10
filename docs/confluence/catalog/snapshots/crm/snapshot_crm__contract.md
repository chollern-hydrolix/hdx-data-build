# snapshot_crm__contract

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | snapshot |
| **Strategy** | timestamp |
| **Unique Key** | `contract_id` |
| **Updated At** | `system_modstamp` |

## Depends On

- `fct_crm__contract`

## Columns

| Column | Type | Description |
|---|---|---|
| `contract_id` | character varying(18) |  |
| `contract_number` | text |  |
| `contract_start_date` | date |  |
| `contract_end_date` | date |  |
| `contract_term` | integer |  |
| `status` | text |  |
| `type` | text |  |
| `type_calculated` | text |  |
| `type_reporting` | text |  |
| `replaced_by_new_contract` | boolean |  |
| `previous_contract_id` | text |  |
| `account_id` | text |  |
| `channel` | text |  |
| `hydrolix_product` | text |  |
| `region` | text |  |
| `country` | text |  |
| `activated_effective_date` | date |  |
| `activated_effective_month` | date |  |
| `churn_confirmed_date` | date |  |
| `churn_confirmed_month` | date |  |
| `mrr_gross` | numeric |  |
| `mrr_net` | numeric |  |
| `gmrr_gross` | numeric |  |
| `gmrr_net` | numeric |  |
| `lmrr_gross` | numeric |  |
| `lmrr_net` | numeric |  |
| `tcv_net` | numeric |  |
| `arr` | numeric |  |
| `nrr_gross_2025` | numeric |  |
| `event_nrr_gross_2025` | numeric |  |
| `commit_amount` | numeric |  |
| `commit_type` | text |  |
| `commit_normalized` | float |  |
| `tb_per_month_standard` | numeric |  |
| `tb_per_month_premium` | numeric |  |
| `bytes_per_month_standard` | float |  |
| `bytes_per_month_premium` | float |  |
| `standard_overages_per_gb` | numeric |  |
| `premium_overages_per_gb` | numeric |  |
| `usage_based_billing_confirmed` | boolean |  |
| `contract_start_month` | date |  |
| `contract_end_month` | date |  |
| `contract_active` | boolean |  |
| `fx_rate` | numeric |  |
| `fx_impact_mrr` | numeric |  |
| `customer_count_impact` | numeric |  |
| `akamai_contract_id` | text |  |
| `pricing_calculator_version` | numeric |  |
| `has_overages` | boolean |  |
| `overage_charges` | text |  |
| `created_date` | timestamp |  |
| `last_modified_date` | timestamp |  |
| `contract_short_id` | text |  |
| `dbt_scd_id` | text |  |
| `dbt_updated_at` | timestamp |  |
| `dbt_valid_from` | timestamp |  |
| `dbt_valid_to` | timestamp |  |
| `system_modstamp` | timestamp |  |
| `reporting_start_date` | date |  |
| `reporting_end_date` | date |  |
| `raw_log_retention_standard` | numeric |  |
| `summary_log_retention_standard` | numeric |  |
| `commit_months` | numeric |  |
| `commit_amount_premium` | numeric |  |
| `standard_overages` | numeric |  |
| `premium_overages` | numeric |  |
| `retention_raw_data` | numeric |  |
| `retention_summary_data` | numeric |  |
| `peak_queries_per_minute` | numeric |  |
| `original_contract_start_date` | date |  |
