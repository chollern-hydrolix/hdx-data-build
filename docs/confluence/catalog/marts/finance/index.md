# finance

| Model | Type | Description |
|---|---|---|
| [fct_active_contract](fct_active_contract.md) | table |  |
| [fct_expiration_risk_contract](fct_expiration_risk_contract.md) | table |  |
| [mart_contract_expiration](mart_contract_expiration.md) | table |  |
| [mart_mrr_contracts](mart_mrr_contracts.md) | table | Monthly MRR waterfall by contract and reporting month, used for finance and revenue reporting. One row per (contract_id, reporting_month).
Period boundaries are anchored to the **activated effective date**: reporting_effective_month is always the activated_effective_month, and replacement contract boundaries are capped using the replacement's activated_effective_month (minus one month), not its start date. This aligns MRR recognition with when a contract actually goes live rather than when it is legally dated.
Differences from mart_accounting__mrr_contracts: (1) Period anchor — uses activated effective date; accounting uses contract start date. (2) NRR gross — includes both "Event" and "High Traffic Event" type_reporting contracts; accounting includes only "Event". (3) Usage normalization — bytes used for all commit types except "Billion records per month"; accounting uses bytes only for "TB per Month" or "GB per Month". (4) Columns present here but absent in accounting: standard_overages, premium_overages, total_queries. (5) commit_type is coalesced to "N/A" here; accounting allows null. |
| [stg_crm__mrr_contract](stg_crm__mrr_contract.md) | view |  |
| [stg_crm__next_opportunity_by_account](stg_crm__next_opportunity_by_account.md) | view |  |
| [stg_mrr_contracts](stg_mrr_contracts.md) | table |  |
| [stg_usage__contract_monthly](stg_usage__contract_monthly.md) | table |  |
