# fct_crm__opportunity_daily_snapshot

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | table |

## Depends On

- `dim_day`
- `snapshot_crm__opportunity`
- `snapshot_crm__opportunity`

## Columns

| Column | Type | Description |
|---|---|---|
| `opportunity_id` | character varying(18) |  |
| `opportunity_name` | text |  |
| `close_date` | date |  |
| `stage_name` | text |  |
| `probability` | numeric |  |
| `forecast_category` | text |  |
| `amount` | numeric |  |
| `type` | text |  |
| `type_calculated` | text |  |
| `type_reporting` | text |  |
| `account_id` | text |  |
| `channel` | text |  |
| `hydrolix_product` | text |  |
| `region` | text |  |
| `country` | text |  |
| `mrr_gross` | numeric |  |
| `mrr_net` | numeric |  |
| `gmrr_gross` | numeric |  |
| `lmrr_gross` | numeric |  |
| `tcv_net` | numeric |  |
| `arr` | numeric |  |
| `nrr_gross` | numeric |  |
| `event_nrr_gross_2025` | numeric |  |
| `owner_id` | text |  |
| `is_closed` | boolean |  |
| `is_won` | boolean |  |
| `poc_initiated` | boolean |  |
| `is_poc` | boolean |  |
| `loss_reason` | text |  |
| `lead_source` | text |  |
| `lead_source_details` | text |  |
| `lead_source_details_other` | text |  |
| `created_date` | timestamp |  |
| `last_modified_date` | timestamp |  |
| `opportunity_short_id` | text |  |
| `system_modstamp` | timestamp |  |
| `poc_start_date` | date |  |
| `use_case` | text |  |
| `age_in_days` | integer |  |
| `stage_duration_in_days` | integer |  |
| `last_stage_change_date` | timestamp |  |
| `grafana_seats` | numeric |  |
| `snapshot_date` | date |  |
