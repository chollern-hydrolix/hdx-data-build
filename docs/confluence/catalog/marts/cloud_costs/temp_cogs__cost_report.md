# temp_cogs__cost_report

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | table |

## Depends On

- `fct_cogs__azure_bucket_cost`
- `fct_crm__contract`

## Columns

| Column | Type | Description |
|---|---|---|
| `invoice_name` | varchar |  |
| `invoice_month` | date |  |
| `cloud_provider` | text |  |
| `cloud_account` | varchar |  |
| `linode_total` | float |  |
| `linode_premium_discount_total` | float |  |
| `linode_hdx_total` | float |  |
| `invoice_enterprise_discount_total` | float |  |
| `invoice_premium_discount_total` | float |  |
| `invoice_poc_credit_total` | float |  |
| `invoice_promotion_credit_total` | float |  |
| `azure_cost` | float |  |
| `cost_type` | text |  |
| `cluster_hostname` | text |  |
| `deployment_ulid` | text |  |
| `deployment_sfid` | varchar |  |
| `account_name` | text |  |
| `opportunity_name` | text |  |
| `contract_number` | text |  |
| `opportunity_stage_name` | text |  |
| `opportunity_close_date` | date |  |
| `opportunity_id` | varchar |  |
| `contract_id` | varchar |  |
