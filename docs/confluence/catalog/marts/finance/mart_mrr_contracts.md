# mart_mrr_contracts

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | table |

## Depends On

- `dim_month`
- `fct_crm__opportunity`
- `fct_crm__replacement_contract`
- `stg_crm__mrr_contract`
- `stg_crm__mrr_contract`
- `stg_usage__contract_monthly`

## Columns

| Column | Type | Description |
|---|---|---|
| `account_name` | text |  |
| `activated_effective_month` | date |  |
| `contract_start_date` | date |  |
| `contract_end_date` | date |  |
| `contract_term_months` | integer |  |
| `start_month` | timestamptz |  |
| `end_month` | timestamptz |  |
| `previous_contract_start_month` | date |  |
| `previous_contract_end_month` | date |  |
| `previous_contract_type_calculated` | text |  |
| `reporting_effective_month` | timestamptz |  |
| `type` | text |  |
| `channel` | text |  |
| `region` | text |  |
| `country` | text |  |
| `hydrolix_product` | text |  |
| `type_calculated` | text |  |
| `type_reporting` | text |  |
| `churn_month` | date |  |
| `mrr_net` | numeric |  |
| `mrr_gross` | numeric |  |
| `gmrr_net` | float |  |
| `gmrr_gross` | float |  |
| `lmrr_net` | numeric |  |
| `lmrr_gross` | numeric |  |
| `churned_mrr_net` | numeric |  |
| `churned_mrr_gross` | numeric |  |
| `account_id` | character varying(18) |  |
| `contract_number` | text |  |
| `contract_id` | character varying(18) |  |
| `status` | text |  |
| `commit_amount` | numeric |  |
| `commit_type` | text |  |
| `fx_rate` | numeric |  |
| `fx_impact_mrr` | numeric |  |
| `customer_count_impact` | numeric |  |
| `activated_effective_date` | date |  |
| `replacement_start_month` | date |  |
| `replacement_end_month` | date |  |
| `lead_source` | text |  |
| `lead_source_details` | text |  |
| `lead_source_details_other` | text |  |
| `sold_by` | text |  |
| `contract_active` | boolean |  |
| `akamai_sales_rep` | text |  |
| `primary_industry` | text |  |
| `primary_sub_industry` | text |  |
| `opportunity_id` | character varying(18) |  |
| `reporting_month` | date |  |
| `reporting_quarter` | date |  |
| `reporting_quarter_label` | text |  |
| `contract_overlaps_previous` | boolean |  |
| `beginning_customers` | integer |  |
| `new_customers` | integer |  |
| `churned_customers` | integer |  |
| `ending_customers` | integer |  |
| `beginning_mrr_net` | float |  |
| `new_mrr_net` | numeric |  |
| `expansion_mrr_net` | float |  |
| `downgrade_mrr_net` | numeric |  |
| `churn_mrr_net` | numeric |  |
| `fx_impact_mrr_net` | numeric |  |
| `beginning_mrr_gross` | float |  |
| `new_mrr_gross` | numeric |  |
| `expansion_mrr_gross` | float |  |
| `downgrade_mrr_gross` | numeric |  |
| `churn_mrr_gross` | numeric |  |
| `fx_impact_mrr_gross` | numeric |  |
| `ending_mrr_gross` | float |  |
| `ending_mrr_net` | float |  |
| `ending_arr_gross` | float |  |
| `ending_arr_net` | float |  |
| `nrr_gross` | numeric |  |
| `total_bytes` | float |  |
| `total_rows` | float |  |
| `total_usage_normalized` | float |  |
| `cumulative_bytes` | float |  |
| `cumulative_rows` | float |  |
| `cumulative_usage_normalized` | float |  |
| `max_qpm` | float |  |
| `total_queries` | float |  |
| `cumulative_max_qpm` | float |  |
| `mrr_band` | text |  |
| `usage_band` | text |  |
