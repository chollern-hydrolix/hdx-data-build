# cloud_costs

| Model | Type | Description |
|---|---|---|
| [dim_cluster_to_deployment](dim_cluster_to_deployment.md) | table |  |
| [fct_akm__invoice_item](fct_akm__invoice_item.md) | table |  |
| [fct_aws__invoice_item](fct_aws__invoice_item.md) | view |  |
| [fct_azr__invoice_item](fct_azr__invoice_item.md) | view |  |
| [fct_cogs__akamai_deployment_cost](fct_cogs__akamai_deployment_cost.md) | table |  |
| [fct_cogs__azure_bucket_cost](fct_cogs__azure_bucket_cost.md) | table |  |
| [fct_cogs__cloud_infrastructure](fct_cogs__cloud_infrastructure.md) | view |  |
| [fct_cogs__cloud_infrastructure_summary](fct_cogs__cloud_infrastructure_summary.md) | view |  |
| [fct_gcp__invoice_item](fct_gcp__invoice_item.md) | view |  |
| [int_cogs__ie_bucket_with_contract](int_cogs__ie_bucket_with_contract.md) | table |  |
| [mart_cogs__daily_contract_margin](mart_cogs__daily_contract_margin.md) | incremental | Daily margin model at (contract_id, deployment_sfid, reporting_date) grain. The (contract, month) grain is driven by the UNION of (1) every (contract, month) in mart_mrr_contracts — guaranteeing 0% MRR variance vs that source — and (2) every (contract, month) where fct_crm__contract_deployment_history says the contract is active — ensuring cost-eligible months still appear even when no MRR has landed yet. Contract/deployment/account attributes are looked up from fct_crm__contract LEFT JOINed to CDH; contracts without a deployment in CDH have deployment_sfid = NULL and NULL cost columns. Pairs daily Linode cost estimates (from stg_linode_instance_billing_daily, allocated via dim_cluster_to_deployment) with pro-rated monthly invoiced Linode cost, Azure bucket cost, and MRR. Estimated and invoiced Linode columns sit side-by-side to enable variance analysis. Built incrementally on a current+previous-month window. |
| [mart_cogs__monthly_contract_margin](mart_cogs__monthly_contract_margin.md) | table | Monthly margin model joining contract/deployment dimensions with MRR revenue and invoiced cloud infrastructure costs (Linode/Akamai + Azure bucket). One row per (deployment_sfid, reporting_month). |
| [mart_cogs__monthly_cost_report](mart_cogs__monthly_cost_report.md) | incremental | Permanent monthly cost report consolidating Linode invoiced costs (dedicated and shared-cluster allocated), Linode invoice discounts (POC, PROMOTION, PREMIUM, ENTERPRISE), and Azure bucket costs. One row per (invoice_name, invoice_month, cluster_hostname, bucket_name, cost_type, deployment_sfid, contract_id). Unallocated upstream rows pass through with 'N/A' deployment_sfid rather than being synthesized into a separate UNALLOCATED row (matches temp_cogs__cost_report semantics). Built incrementally on a current+previous-month window. |
| [rpt_daily_linode_dev_costs](rpt_daily_linode_dev_costs.md) | table |  |
| [stg_azure__resource_group_name_map](stg_azure__resource_group_name_map.md) | table |  |
| [stg_contract_azure_usage](stg_contract_azure_usage.md) | view |  |
| [stg_contract_linode_usage](stg_contract_linode_usage.md) | view |  |
| [stg_daily_shared_cluster_project_usage_estimate_pct](stg_daily_shared_cluster_project_usage_estimate_pct.md) | view |  |
| [stg_linode__daily_shared_cluster_allocation](stg_linode__daily_shared_cluster_allocation.md) | view |  |
| [stg_linode__monthly_shared_cluster_allocation](stg_linode__monthly_shared_cluster_allocation.md) | view |  |
| [stg_linode_instance_billing](stg_linode_instance_billing.md) | table |  |
| [stg_linode_instance_billing_daily](stg_linode_instance_billing_daily.md) | incremental |  |
| [stg_linode_instance_with_shutdown](stg_linode_instance_with_shutdown.md) | table |  |
| [temp_cogs__cost_report](temp_cogs__cost_report.md) | table |  |
