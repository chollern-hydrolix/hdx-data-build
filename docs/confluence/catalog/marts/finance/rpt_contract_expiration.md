# rpt_contract_expiration

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | table |

## Depends On

- `mart_contract_expiration`

## Columns

| Column | Type | Description |
|---|---|---|
| `account_name` | varchar |  |
| `start_month` | date |  |
| `end_month` | date |  |
| `mrr_gross` | float |  |
| `type` | varchar |  |
| `type_calculated` | text |  |
| `activated_effective_month` | date |  |
| `reporting_month` | date |  |
| `account_id` | varchar |  |
| `contract_id` | varchar |  |
| `contract_number` | varchar |  |
| `replaced_by_new_contract` | boolean |  |
| `renewal_contract_id` | varchar |  |
| `renewal_contract_number` | varchar |  |
| `renewal_mrr_gross` | float |  |
| `renewal_type` | varchar |  |
| `replaced_by_draft_contract` | boolean |  |
| `is_event` | boolean |  |
| `renewal_category` | text |  |
| `channel` | varchar |  |
| `region` | varchar |  |
| `country` | varchar |  |
| `hydrolix_product` | varchar |  |
| `metric` | text |  |
| `value` | float |  |
| `label` | text |  |
| `label_with_sort` | text |  |
