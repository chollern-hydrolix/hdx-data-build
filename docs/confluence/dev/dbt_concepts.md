# dbt Concepts

## Project Structure

```
dbt_analytics/
├── models/
│   ├── marts/          # Analytics-ready output tables
│   │   ├── crm/
│   │   ├── finance/
│   │   ├── account_health/
│   │   ├── accounting/
│   │   ├── cloud_costs/
│   │   └── salesops/
│   ├── sources/        # Source schema declarations (.yml)
│   └── shared_dimensions/
├── snapshots/          # SCD Type 2 historical tracking
│   └── crm/
├── seeds/              # Static CSV reference data
├── macros/             # Reusable Jinja2 macros
└── tests/              # Custom generic tests
```

## Sources

Raw data landing in the database is declared as sources in `models/sources/*.yml`. This tells dbt where to find upstream data without managing it directly.

```sql
-- Reference a source table in a model
select * from {{ source('raw_salesforce', 'account') }}
```

Source schemas in use:

| Source | Schema | Data |
|---|---|---|
| `raw_salesforce` | `raw_salesforce` | CRM data synced from Salesforce |
| `argus` | `argus` | Internal usage telemetry |
| `linode` | `linode` | Linode infrastructure data |
| `internal` | `internal` | Internal reference data |

## Models

Models are SQL files that dbt compiles and executes. Cross-model dependencies use `{{ ref() }}` — dbt resolves the correct schema automatically and builds a dependency graph to determine execution order.

```sql
select * from {{ ref('fct_crm__contract') }}
```

**Materialization types used in this project:**

| Type | Behaviour | When used |
|---|---|---|
| `table` | Drop and recreate on every run | Most marts |
| `view` | Recreated as a SQL view | Lightweight staging models |
| `incremental` | Insert/update new rows only | High-volume append-only data |
| `snapshot` | SCD Type 2 history table | CRM entity tracking |

Materialization is set per-model via config:
```sql
{{ config(materialized='table') }}
```

## Snapshots

Snapshots track how rows change over time using SCD Type 2. Each snapshot adds `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`, and `dbt_updated_at` columns.

This project uses the `timestamp` strategy — dbt checks the `system_modstamp` column to detect changes:

```sql
{% snapshot snapshot_crm__opportunity %}
{{ config(
    unique_key='opportunity_id',
    strategy='timestamp',
    updated_at='system_modstamp'
) }}
select * from {{ ref('fct_crm__opportunity') }}
{% endsnapshot %}
```

Snapshots run daily at 02:00 UTC via ECS. **Do not run snapshots in two environments simultaneously** — this will create duplicate history rows.

## Naming Conventions

Model names follow this structure:

```
{prefix}_{domain}__{entity_name}
```

The double underscore (`__`) separates the domain from the entity name. Example: `fct_crm__contract`, `mart_cogs__monthly_contract_margin`.

### Prefixes

| Prefix | Meaning |
|---|---|
| `fct_` | Fact table — events or transactions |
| `dim_` | Dimension table — descriptive attributes |
| `mart_` | Aggregated reporting model |
| `stg_` | Staging model — light cleaning/renaming of source data |
| `rpt_` | Report-specific model |
| `int_` | Intermediate model — not intended for direct consumption |
| `snapshot_` | SCD Type 2 snapshot |

### Domains

| Domain | Meaning |
|---|---|
| `crm` | Salesforce CRM data — contracts, deployments, opportunities, accounts |
| `cogs` | Cloud cost of goods sold — infrastructure margin and spend |
| `finance` | Revenue, MRR, contract expiration |
| `akm` | Akamai invoice and billing data |
| `aws` | AWS invoice and billing data |
| `azr` | Azure invoice and billing data |
| `gcp` | GCP invoice and billing data |
| `ie` | Internal infrastructure / IE bucket data |

## Packages

External packages are declared in `packages.yml`. Run `make dbt-deps` to install them.

| Package | Version | Usage |
|---|---|---|
| `dbt_utils` | >=1.0.0,<2.0.0 | Generic test helpers, SQL utilities |