# stg_next_opportunity_by_account

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | view |

## Depends On

- `dim_account`
- `fct_opportunity`

## Columns

| Column | Type | Description |
|---|---|---|
| `opportunity_name` | varchar |  |
| `owner_name` | varchar |  |
| `stage_name` | varchar |  |
| `mrr_gross` | float |  |
| `close_date` | date |  |
| `probability` | float |  |
| `account_name` | varchar |  |
| `opportunity_id` | varchar |  |
| `account_id` | varchar |  |
| `type_reporting` | varchar |  |
| `_rank` | bigint |  |
