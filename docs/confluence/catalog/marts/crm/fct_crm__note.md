# fct_crm__note

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
| `title` | text |  |
| `body` | text |  |
| `owner_name` | text |  |
| `created_by_name` | text |  |
| `last_modified_by_name` | text |  |
| `created_date` | timestamp |  |
| `last_modified_date` | timestamp |  |
| `note_id` | character varying(18) |  |
| `parent_id` | text |  |
| `owner_id` | text |  |
| `created_by_id` | text |  |
| `last_modified_by_id` | text |  |
| `parent_prefix` | text |  |
| `parent_type` | text |  |
| `account_name` | text |  |
| `account_id` | varchar |  |
