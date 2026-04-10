# ECS Scheduled Builds

Schedules are defined in `ebs/tasks.yml` and deployed to AWS EventBridge Scheduler via `python ebs/deploy_scheduled_tasks.py`. All times are UTC.

| Schedule | Task Definition | Frequency         | Description                                                                                                |
|---|---|-------------------|------------------------------------------------------------------------------------------------------------|
| `dbt.snapshot.crm` | `dbt-snapshot` | Daily at 02:00    | SCD Type 2 snapshots for all CRM source tables                                                             |
| `dbt.build.crm` | `dbt-run-crm` | Every hour at :30 | CRM mart models (*this mart will build at the top of the hour because the `finance` mart depends on `crm`*) |
| `dbt.build.finance` | `dbt-build-finance` | Every hour        | Finance mart models                                                                                        |
| `dbt.build.mart-monthly-customer-usage` | `dbt-build-mart-monthly-customer-usage` | Every hour        | `mart_monthly_customer_usage` and all upstream dependencies                                                |
| `dbt.build.medium-priority` | `dbt-build-medium-priority` | Every 4 hours     | Account health, accounting, cloud costs, salesops marts                                                    |

## Snapshots

Snapshots run once daily at 02:00 UTC, before any model builds. They track historical changes (SCD Type 2) on the following CRM tables using a `timestamp` strategy on `system_modstamp`:

- `snapshot_crm__opportunity`
- `snapshot_crm__case`
- `snapshot_crm__contract`
- `snapshot_crm__deployment`
- `snapshot_crm__lead`

## Infrastructure

| | |
|---|---|
| **ECS Cluster** | `arn:aws:ecs:us-east-2:570204184505:cluster/hdx-data-build` |
| **Scheduler Group** | `hdx-data-build` |
| **Scheduler Role** | `Amazon_EventBridge_Scheduler_ECS_Role` |
| **Subnets** | `subnet-0e69b634bf2460fff`, `subnet-0bdaf99fa9526ae45` |
| **Security Groups** | `sg-031751a98314fa5b9`, `sg-04ccc6674bc993dc7` |
| **VPC** | `vpc-0e138559715c85359` |

## Logs

Each task writes to its own CloudWatch log group:

| Task | Log Group |
|---|---|
| `dbt-snapshot` | `/ecs/dbt-snapshot` |
| `dbt-run-crm` | `/ecs/dbt-run-crm` |
| `dbt-build-finance` | `/ecs/dbt-build-finance` |
| `dbt-build-mart-monthly-customer-usage` | `/ecs/dbt-build-mart-monthly-customer-usage` |
| `dbt-build-medium-priority` | `/ecs/dbt-build-medium-priority` |

Run results are also logged to `dbt_logs.run_summary` and `dbt_logs.run_node` in the `analytics` PostgreSQL database.