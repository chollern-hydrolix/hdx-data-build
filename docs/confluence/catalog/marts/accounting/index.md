# accounting

| Model | Type | Description |
|---|---|---|
| [mart_accounting__mrr_contracts](mart_accounting__mrr_contracts.md) | table | Monthly MRR waterfall by contract and reporting month, used for accounting and financial statement purposes. One row per (contract_id, reporting_month).
Period boundaries are anchored to the **contract start date**: reporting_effective_month is always start_month, and replacement contract boundaries are capped using the replacement's start_month (minus one month), not its activated effective date. This aligns MRR recognition with the contract's legal term as written, which is required for accounting treatment.
Differences from mart_mrr_contracts: (1) Period anchor — uses contract start date; finance uses activated effective date. (2) NRR gross — includes only "Event" type_reporting contracts; finance also includes "High Traffic Event". (3) Usage normalization — bytes used only for "TB per Month" or "GB per Month" commit types; finance uses bytes for all commit types except "Billion records per month". (4) Columns present in mart_mrr_contracts but absent here: standard_overages, premium_overages, total_queries. (5) commit_type is not coalesced and may be null; finance coalesces to "N/A". |
| [overage_billing](overage_billing.md) | table |  |
