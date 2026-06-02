# mart_cogs__daily_contract_margin

Daily margin model at (contract_id, deployment_sfid, reporting_date) grain. The (contract, month) grain is driven by the UNION of (1) every (contract, month) in mart_mrr_contracts — guaranteeing 0% MRR variance vs that source — and (2) every (contract, month) where fct_crm__contract_deployment_history says the contract is active — ensuring cost-eligible months still appear even when no MRR has landed yet. Contract/deployment/account attributes are looked up from fct_crm__contract LEFT JOINed to CDH; contracts without a deployment in CDH have deployment_sfid = NULL and NULL cost columns. Pairs daily Linode cost estimates (from stg_linode_instance_billing_daily, allocated via dim_cluster_to_deployment) with pro-rated monthly invoiced Linode cost, Azure bucket cost, and MRR. Estimated and invoiced Linode columns sit side-by-side to enable variance analysis. Built incrementally on a current+previous-month window.

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | incremental |

## Depends On

- `dim_cluster_to_deployment`
- `dim_cluster_to_deployment`
- `dim_crm__account`
- `dim_day`
- `dim_month`
- `fct_cogs__akamai_deployment_cost`
- `fct_cogs__akamai_deployment_cost`
- `fct_cogs__azure_bucket_cost`
- `fct_cogs__azure_bucket_cost`
- `fct_crm__contract`
- `fct_crm__contract_deployment_history`
- `fct_crm__contract_deployment_history`
- `fct_crm__deployment`
- `fct_usage__deployment_daily`
- `mart_mrr_contracts`
- `mart_mrr_contracts`
- `stg_linode_instance_billing_daily`
- `stg_linode_instance_billing_daily`

## Columns

| Column | Type | Description |
|---|---|---|
| `margin_daily_id` | text | Surrogate hash of (contract_id, deployment_sfid, reporting_date). Stable across ETL reloads. |
| `reporting_date` | date | The calendar day this row represents. |
| `reporting_month` | date | First day of the month containing reporting_date. Convenience column for monthly rollups. |
| `days_in_month` | integer | Total number of days in reporting_month. Divisor for all `_prorated` columns. |
| `account_id` | character varying(18) | Salesforce ID of the customer account. |
| `account_name` | text | Salesforce account name for the customer. |
| `region` | text | Sales region from the contract. |
| `hydrolix_product` | text | Product line on the contract. |
| `commit_type` | text | Unit of commit_amount. |
| `commit_amount` | numeric | Contracted commitment quantity for the commit_type unit. |
| `contract_id` | character varying(18) | Salesforce ID of the contract. |
| `contract_start_date` | date | Start date of the active contract. |
| `contract_end_date` | date | End date of the active contract. |
| `deployment_sfid` | character varying(18) | Salesforce ID of the deployment. NULL when the contract has no row in fct_crm__contract_deployment_history (e.g., revenue-only contracts or non-activated contracts). |
| `deployment_ulid` | text | ULID identifier of the deployment. NULL when deployment_sfid is NULL. |
| `estimated_daily_linode_cost` | float | Estimated daily Linode infrastructure cost derived from stg_linode_instance_billing_daily and allocated to this deployment via dim_cluster_to_deployment, then divided evenly across the contracts that share the deployment for the month so naive rollups aren't double-counted. Forward-looking estimate, not invoiced. |
| `estimated_daily_linode_hdx_cost` | float | HDX-priced equivalent of estimated_daily_linode_cost; same contract-share allocation applied. |
| `monthly_invoiced_linode_cost` | float | Sum of invoiced Linode cost for this (deployment, month). Same value repeated across every day AND every contract sharing the deployment — use max() per (deployment_sfid, reporting_month) before summing for deployment-level monthly rollups. |
| `monthly_invoiced_linode_hdx_cost` | float | HDX-priced equivalent of monthly_invoiced_linode_cost. Same repetition rule applies. |
| `monthly_invoiced_premium_discount_linode_cost` | float | Premium-discount-priced equivalent of monthly_invoiced_linode_cost. Same repetition rule applies. |
| `invoiced_daily_linode_cost_prorated` | numeric | monthly_invoiced_linode_cost / days_in_month / contracts_sharing_the_deployment_in_month. Summing this across all (contract, deployment, day) rows reproduces the upstream monthly total without double-counting deployments that span multiple contracts. |
| `invoiced_daily_linode_hdx_cost_prorated` | numeric | HDX-priced equivalent of invoiced_daily_linode_cost_prorated. |
| `invoiced_daily_premium_discount_linode_cost_prorated` | numeric | Premium-discount equivalent of invoiced_daily_linode_cost_prorated. |
| `monthly_azure_bucket_cost` | float | Sum of invoiced Azure bucket cost for this (deployment, month). Same value repeated across every day AND every contract sharing the deployment — use max() per (deployment_sfid, reporting_month) before summing for deployment-level monthly rollups. |
| `daily_azure_bucket_cost_prorated` | numeric | monthly_azure_bucket_cost / days_in_month / contracts_sharing_the_deployment_in_month. Sums correctly across the entire mart to upstream Azure totals. |
| `ending_mrr_gross` | float | End-of-month gross MRR for the contract. Same value repeated across every day in the month. |
| `daily_mrr_prorated` | numeric | ending_mrr_gross / days_in_month. |
| `total_bytes` | float | Total ingested bytes used in MRR computation (monthly grain, repeated per day). |
| `total_rows` | float | Total ingested rows used in MRR computation (monthly grain, repeated per day). |
