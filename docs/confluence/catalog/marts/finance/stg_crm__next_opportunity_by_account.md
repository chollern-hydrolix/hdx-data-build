# stg_crm__next_opportunity_by_account

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | view |

## Depends On

- `dim_crm__account`
- `fct_crm__opportunity`

## Columns

| Column | Type | Description |
|---|---|---|
| `opportunity_name` | text |  |
| `owner_name` | text |  |
| `stage_name` | text |  |
| `mrr_gross` | numeric |  |
| `close_date` | date |  |
| `probability` | numeric |  |
| `account_name` | text |  |
| `opportunity_id` | character varying(18) |  |
| `account_id` | text |  |
| `type_reporting` | text |  |
| `_rank` | bigint |  |
