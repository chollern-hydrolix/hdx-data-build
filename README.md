# hdx-data-build

dbt analytics pipeline for Hydrolix business intelligence. Transforms raw data from Salesforce and Argus into reporting-ready marts covering CRM, finance, cloud costs, and account health.

Runs locally for development and deploys to AWS ECS Fargate for production.

## Data Sources

| Source | Schema | Description |
|--------|--------|-------------|
| `raw_salesforce` | Salesforce CRM | Accounts, contracts, opportunities, deployments, leads, cases |
| `argus` | Hydrolix usage platform | Daily table and query usage metrics |
| `internal` | Internal metadata | Salesforce object/field definitions |
| `linode` | Akamai/Linode | Cloud infrastructure billing and allocation |

## Marts

| Area | Models | Description |
|------|--------|-------------|
| `crm` | contracts, deployments, opportunities, leads, cases, activities | CRM facts and snapshots |
| `finance` | MRR, active contracts, expiration risk, replacement contracts | Revenue and contract reporting |
| `cloud_costs` | AWS, GCP, Azure, Akamai invoice items, COGS summaries | Cloud infrastructure cost of goods sold |
| `account_health` | daily usage, deployment health, project/deployment metrics | Customer health and usage tracking |
| `accounting` | MRR contracts, overage billing | Finance reporting |
| `salesops` | pipeline snapshots | Sales pipeline tracking |

## Setup

### Prerequisites

- Python 3.13+
- PostgreSQL access (credentials in `.env`)
- direnv (for automatic env var loading)

### Install

```bash
# Activate virtual environment
source .venv/bin/activate

# Install dbt packages
cd dbt_analytics
dbt deps
```

### Environment

Environment variables are loaded automatically via direnv. The `.env` file at the project root must define:

```
DBT_HOST=<postgres host>
DBT_USER=<postgres user>
DBT_PASSWORD=<postgres password>
DBT_DATABASE=postgres
DBT_PORT=5432
DBT_TARGET=local   # or prod
```

## dbt Commands

All commands run from the `dbt_analytics/` directory.

```bash
cd dbt_analytics

dbt debug                     # Validate connection
dbt run                       # Run all models
dbt test                      # Run all tests
dbt run -s <model_name>       # Run a single model
dbt test -s <model_name>      # Test a single model
dbt clean                     # Remove build artifacts
```

## Targets

| Target | Database | Schema | Use |
|--------|----------|--------|-----|
| `local` | `postgres` | `dbt_chollern` | Local development |
| `prod` | `postgres` | `analytics` | Production (ECS Fargate) |