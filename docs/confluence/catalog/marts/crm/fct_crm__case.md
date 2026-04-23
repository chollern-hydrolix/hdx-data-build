# fct_crm__case

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | table |

## Depends On

- `dim_crm__account`

## Columns

| Column | Type | Description |
|---|---|---|
| `case_id` | character varying(18) |  |
| `case_number` | text |  |
| `account_name` | text |  |
| `contact_name` | text |  |
| `owner_name` | text |  |
| `subject` | text |  |
| `type` | text |  |
| `status` | text |  |
| `reason` | text |  |
| `origin` | text |  |
| `priority` | text |  |
| `closed_date` | timestamp |  |
| `issue_type` | text |  |
| `customer_priority` | text |  |
| `is_closed` | boolean |  |
| `is_escalated` | boolean |  |
| `supplied_name` | text |  |
| `supplied_email` | text |  |
| `supplied_phone` | text |  |
| `supplied_company` | text |  |
| `contact_email` | text |  |
| `contact_phone` | text |  |
| `master_record_id` | text |  |
| `account_id` | text |  |
| `contact_id` | text |  |
| `owner_id` | text |  |
| `created_by_id` | text |  |
| `last_modified_by_id` | text |  |
| `created_date` | timestamp |  |
| `last_modified_date` | timestamp |  |
| `contract_short_id` | text |  |
| `system_modstamp` | timestamp |  |
