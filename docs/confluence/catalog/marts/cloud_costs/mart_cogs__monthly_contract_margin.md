# mart_cogs__monthly_contract_margin

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | table |

## Depends On

- `dim_crm__account`
- `dim_month`
- `fct_cogs__akamai_deployment_cost`
- `fct_cogs__azure_bucket_cost`
- `fct_crm__contract`
- `fct_crm__contract_deployment_history`
- `fct_crm__deployment`
- `mart_mrr_contracts`

## Columns

| Column | Type | Description |
|---|---|---|
| `reporting_month` | date |  |
| `account_name` | text |  |
| `region` | text |  |
| `contract_start_date` | date |  |
| `contract_end_date` | date |  |
| `hydrolix_product` | text |  |
| `commit_amount` | numeric |  |
| `commit_type` | text |  |
| `deployment_sfid` | character varying(18) |  |
| `deployment_ulid` | text |  |
| `contract_id` | character varying(18) |  |
| `account_id` | character varying(18) |  |
| `ending_mrr_gross` | float |  |
| `total_linode_cost` | float |  |
| `premium_discount_linode_cost` | float |  |
| `hdx_linode_cost` | float |  |
| `azure_bucket_cost` | float |  |
| `total_bytes` | float |  |
| `total_rows` | float |  |
