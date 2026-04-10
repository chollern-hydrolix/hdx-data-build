# ECS Deployment

## Overview

Production dbt builds run as AWS ECS Fargate tasks. Each task pulls the latest container image from ECR, runs a dbt command, and exits. Tasks are stateless — no data is stored in the container.

## Deploying a New Image

```bash
make deploy
```

This runs three steps in sequence:
1. `docker-build` — builds the image for `linux/amd64`
2. `docker-push` — pushes to ECR (`hdx-business-systems/data-build:latest`)
3. `register-tasks` — registers all task definitions in `ecs/task_definitions/`

Run this whenever model code, the `Dockerfile`, or `entrypoint.sh` changes.

## Task Definitions

Task definitions are JSON files in `ecs/task_definitions/`. Each defines the dbt command, CPU/memory, secrets, and log group for one type of build.

| Task Definition | dbt Command | CPU | Memory |
|---|---|---|---|
| `dbt-snapshot` | `snapshot` | 256 | 512 |
| `dbt-run-crm` | `build --select path:models/marts/crm` | 256 | 512 |
| `dbt-build-finance` | `build --select path:models/marts/finance` | 256 | 512 |
| `dbt-build-mart-monthly-customer-usage` | `build --select +mart_monthly_customer_usage` | 256 | 512 |
| `dbt-build-medium-priority` | `build --select path:models/marts/account_health path:models/marts/accounting ...` | 512 | 1024 |
| `dbt-run-full` | `run` | 512 | 1024 |

## Secrets

Database credentials are stored in AWS Secrets Manager at `hdx/dbt` and injected as environment variables at task startup:

| Variable | Source |
|---|---|
| `DBT_HOST` | Secrets Manager |
| `DBT_USER` | Secrets Manager |
| `DBT_PASSWORD` | Secrets Manager |
| `DBT_TARGET` | Hardcoded to `prod` in task definition |

## Scheduling

Builds are triggered by AWS EventBridge Scheduler. Schedules are defined in `ebs/tasks.yml` and deployed via:

```bash
python ebs/deploy_scheduled_tasks.py
```

See the [Schedules](../schedules/index.md) page for the full schedule.

## Running a Task Manually

```bash
make tmp-run-crm
```

To run other task definitions manually, use the AWS CLI directly:

```bash
aws ecs run-task \
  --cluster arn:aws:ecs:us-east-2:570204184505:cluster/hdx-data-build \
  --task-definition <task-definition-name> \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-0e69b634bf2460fff,subnet-0bdaf99fa9526ae45],securityGroups=[sg-031751a98314fa5b9,sg-04ccc6674bc993dc7],assignPublicIp=ENABLED}" \
  --region us-east-2 \
  --profile AdministratorAccess-570204184505
```

## Network

ECS tasks run in the same VPC as the RDS instance and Lambda functions (`vpc-0e138559715c85359`). The security groups used are shared with the existing Lambda infrastructure, which is what grants RDS access.

| | |
|---|---|
| **VPC** | `vpc-0e138559715c85359` |
| **Subnets** | `subnet-0e69b634bf2460fff`, `subnet-0bdaf99fa9526ae45` |
| **Security Groups** | `sg-031751a98314fa5b9`, `sg-04ccc6674bc993dc7` |