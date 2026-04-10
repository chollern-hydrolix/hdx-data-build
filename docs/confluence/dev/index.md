# Developer Guide

This guide covers everything needed to develop, run, and deploy the HDX data build pipeline.

## Contents

| | |
|---|---|
| [Local Development](local_development.md) | Setting up your environment and running dbt locally |
| [dbt Concepts](dbt_concepts.md) | How dbt is structured and used in this project |
| [Data Models](data_models.md) | Overview of marts, their purpose, and refresh schedules |
| [ECS Deployment](ecs_deployment.md) | How builds run in production via ECS Fargate |
| [Observability](observability.md) | Logs, run tracking, and monitoring scheduled jobs |

## Project Summary

This project runs dbt transformations against a PostgreSQL database hosted on AWS RDS. Source data is loaded externally (Salesforce via a separate ETL, Argus, Linode, etc.) and lands in raw schemas. dbt transforms this data into analytics-ready marts consumed by dashboards and reporting tools.

Builds are scheduled via AWS EventBridge Scheduler, which triggers ECS Fargate tasks on a per-mart cadence. Run results are logged to a `dbt_logs` schema in PostgreSQL for observability.

## Key Tech

| | |
|---|---|
| **Transformation** | dbt-core 1.11.7, dbt-postgres 1.10.0 |
| **Database** | PostgreSQL on AWS RDS (`hdx-business-analytics`) |
| **Compute** | AWS ECS Fargate |
| **Scheduling** | AWS EventBridge Scheduler |
| **Container Registry** | AWS ECR |
| **Secrets** | AWS Secrets Manager |