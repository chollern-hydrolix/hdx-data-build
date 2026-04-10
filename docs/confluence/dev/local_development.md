# Local Development

## Prerequisites

- Python 3.13+
- Access to the RDS instance (VPN or direct)
- AWS credentials configured for `AdministratorAccess-570204184505`
- The `dbt` database user password (retrieve from dbt Cloud connection settings)

## First-Time Setup

**1. Activate the virtual environment**
```bash
source .venv/bin/activate
```

**2. Configure environment variables**

Copy `.env` and fill in the `dbt` user password:
```bash
DBT_USER=dbt
DBT_PASSWORD=<password>
DBT_HOST=hdx-business-analytics.c1d0xkjgk3o3.us-east-2.rds.amazonaws.com
DBT_TARGET=local
DBT_DATABASE=analytics
```

**3. Install dbt packages**
```bash
make dbt-deps
```

**4. Verify your connection**
```bash
make dbt-debug
```

## Running dbt

All dbt commands are wrapped in the Makefile. The `local` target writes to the `dbt_chollern` schema — it is safe to run at any time without affecting production.

| Command | Description |
|---|---|
| `make dbt-run` | Run all models |
| `make dbt-build` | Run all models + tests |
| `make dbt-test` | Run data quality tests |
| `make dbt-snapshot` | Run all snapshots |
| `make dbt-run-model model=<name>` | Run a single model |
| `make dbt-build-model model=<name>` | Build + test a single model |
| `make dbt-clean` | Remove `target/` and `dbt_packages/` |

To run against production instead of local:
```bash
make dbt-run DBT_TARGET=prod
```

## Targets

| Target | Schema | Purpose |
|---|---|---|
| `local` | `dbt_chollern` | Development — safe to run freely |
| `prod` | `analytics` | Production — used by ECS scheduled builds |

## Generating Docs

```bash
make dbt-docs-export env=prod   # generates + exports markdown to docs/confluence/catalog/
make dbt-docs-serve             # serves interactive docs at http://127.0.0.1:8080
```

## Important Notes

- **Snapshots**: do not run `dbt snapshot --target prod` locally unless you are performing a deliberate cutover. Snapshots write SCD Type 2 history — running them in two places simultaneously corrupts the record.
- **dbt user ownership**: all production tables are owned by the `dbt` database user. Always run prod builds as this user to avoid ownership conflicts.