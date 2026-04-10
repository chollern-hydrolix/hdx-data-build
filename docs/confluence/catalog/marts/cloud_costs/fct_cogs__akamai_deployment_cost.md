# fct_cogs__akamai_deployment_cost

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | table |

## Depends On

- `fct_akm__invoice_item`
- `stg_linode__monthly_shared_cluster_allocation`

## Columns

| Column | Type | Description |
|---|---|---|
| `invoice_month` | date |  |
| `cluster_hostname` | text |  |
| `cluster_project_name` | text |  |
| `raw_linode_cost` | float |  |
| `raw_premium_discount_linode_cost` | float |  |
| `raw_hdx_linode_cost` | float |  |
| `pro_rated_pct` | float |  |
| `deployment_sfid` | text |  |
| `total_linode_cost` | float |  |
| `premium_discount_linode_cost` | float |  |
| `hdx_linode_cost` | float |  |
