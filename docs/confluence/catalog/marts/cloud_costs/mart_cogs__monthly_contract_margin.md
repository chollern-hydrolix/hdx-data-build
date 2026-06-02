# mart_cogs__monthly_contract_margin

Monthly margin model joining contract/deployment dimensions with MRR revenue and invoiced cloud infrastructure costs (Linode/Akamai + Azure bucket). One row per (deployment_sfid, reporting_month).

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
| `reporting_month` | date | First day of the reporting month (month grain). |
| `account_name` | text | Salesforce account name for the customer. |
| `region` | text | Sales region from the contract. |
| `contract_start_date` | date | Start date of the active contract. |
| `contract_end_date` | date | End date of the active contract. |
| `hydrolix_product` | text | Product line on the contract. |
| `commit_amount` | numeric | Contracted commitment quantity for the commit_type unit. |
| `commit_type` | text | Unit of commit_amount (e.g., 'TB per Month', 'Billion records per month'). |
| `deployment_sfid` | character varying(18) | Salesforce ID of the deployment. |
| `deployment_ulid` | text | ULID identifier of the deployment. |
| `contract_id` | character varying(18) | Salesforce ID of the contract. |
| `account_id` | character varying(18) | Salesforce ID of the customer account. |
| `ending_mrr_gross` | float | End-of-month gross monthly recurring revenue for the contract. |
| `total_linode_cost` | float | Sum of invoiced Linode costs allocated to this deployment for the month (post pro-ration for shared clusters). |
| `premium_discount_linode_cost` | float | Sum of premium-discount-priced Linode costs for the deployment-month. |
| `hdx_linode_cost` | float | HDX-priced Linode cost component for the deployment-month. |
| `azure_bucket_cost` | float | Allocated Azure bucket storage cost for the deployment-month. |
| `total_bytes` | float | Total ingested bytes used in MRR computation (monthly grain). |
| `total_rows` | float | Total ingested rows used in MRR computation (monthly grain). |
