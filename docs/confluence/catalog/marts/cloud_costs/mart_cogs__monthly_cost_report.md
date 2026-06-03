# mart_cogs__monthly_cost_report

Permanent monthly cost report consolidating Linode invoiced costs (dedicated and shared-cluster allocated), Linode invoice discounts (POC, PROMOTION, PREMIUM, ENTERPRISE), and Azure bucket costs. One row per (invoice_name, invoice_month, cluster_hostname, bucket_name, cost_type, deployment_sfid, contract_id). Unallocated upstream rows pass through with 'N/A' deployment_sfid rather than being synthesized into a separate UNALLOCATED row (matches temp_cogs__cost_report semantics). Built incrementally on a current+previous-month window.

## Details

| | |
|---|---|
| **Schema** | `dbt_chollern` |
| **Materialization** | incremental |

## Depends On

- `fct_akm__invoice_item`
- `fct_cogs__azure_bucket_cost`
- `stg_linode__monthly_shared_cluster_allocation`

## Columns

| Column | Type | Description |
|---|---|---|
| `cost_report_id` | text | Surrogate hash of the natural composite (invoice_name, invoice_month, cloud_provider, cloud_account, cluster_hostname, bucket_name, cost_type, deployment_sfid, contract_id). Stable across ETL reloads. |
| `invoice_name` | varchar | Source-system invoice label (e.g., the Linode invoice number, or "Jun 2026 Azure Invoice" for Azure). |
| `invoice_month` | date | First day of the month the cost is attributed to. For Linode this is invoice_publish_month minus one month. |
| `cloud_provider` | text | One of 'Linode' or 'Azure'. |
| `cloud_account` | varchar | Cloud account name (e.g., 'prod', 'dev', 'warner', 'trafficpeak-ops', 'azure-prod'). |
| `linode_total` | float | Total Linode cost for this row (post shared-cluster pro-ration if applicable). |
| `linode_premium_discount_total` | float | Linode cost after premium discount, for the row. |
| `linode_hdx_total` | float | HDX-priced Linode cost for the row. |
| `invoice_enterprise_discount_total` | float | Enterprise discount amount (signed, negative for credits). Populated only for Akamai discount rows. |
| `invoice_premium_discount_total` | float | Premium discount amount (signed). Populated only for Akamai discount rows. |
| `invoice_poc_credit_total` | float | POC credit amount (signed). Populated only for Akamai discount rows. |
| `invoice_promotion_credit_total` | float | Promotion credit amount (signed). Populated only for Akamai discount rows. |
| `azure_cost` | float | Azure bucket pre-tax cost for the row. Populated only for Azure rows. |
| `cost_type` | text | Cost classification ('PAID', 'POC', 'S&M', 'R&D', 'SHARED', 'INTERNAL', 'UNKNOWN', 'AKAMAI INVOICE'). Derived from cloud_account, cluster naming conventions, contract dates, and shared-vs-dedicated cluster status. |
| `cluster_hostname` | text | Salesforce-side cluster hostname (e.g., 'my-cluster.trafficpeak.live'). 'N/A' for rows that don't map to a cluster (e.g., Akamai discount lines). |
| `bucket_name` | text | Azure bucket / storage name for Azure rows. NULL for Linode invoice and discount rows. |
| `deployment_ulid` | text | ULID of the deployment. |
| `deployment_sfid` | varchar | Salesforce deployment ID this cost is allocated to. 'N/A' if no deployment mapping was found upstream. |
| `account_name` | text | Salesforce account name. |
| `opportunity_name` | text | Opportunity name. |
| `contract_number` | text | Contract number string. |
| `opportunity_stage_name` | text | Opportunity stage (e.g., 'Closed Won'). |
| `opportunity_close_date` | date | Opportunity close date. |
| `opportunity_id` | varchar | Opportunity ID associated with the deployment. |
| `contract_id` | varchar | Salesforce contract ID associated with the deployment. 'N/A' if no contract is linked. |
