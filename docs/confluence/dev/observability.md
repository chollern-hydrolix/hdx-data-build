# Observability

## Run Logging

Every dbt invocation is logged to PostgreSQL by `data_build/runner.py` after the dbt process exits. Two tables are written to:

**`dbt_logs.run_summary`** — one row per invocation

| Column | Description |
|---|---|
| `invocation_id` | Unique ID assigned by dbt |
| `command` | Full dbt command that was run |
| `target` | dbt target (`prod` or `local`) |
| `started_at` | When the run started |
| `completed_at` | When dbt finished |
| `elapsed_seconds` | Total run duration |
| `nodes_success` | Count of models that succeeded |
| `nodes_error` | Count of models that errored |
| `nodes_skip` | Count of models skipped |

**`dbt_logs.run_node`** — one row per model/test/snapshot

| Column | Description |
|---|---|
| `invocation_id` | Links to `run_summary` |
| `unique_id` | dbt node ID (e.g. `model.dbt_analytics.fct_crm__contract`) |
| `node_type` | `model`, `test`, `snapshot` |
| `status` | `success`, `error`, `skipped`, `warn` |
| `execution_seconds` | Time taken for this node |
| `message` | Error message if failed |

## CloudWatch Logs

Each task definition writes to its own log group. Logs include dbt's full stdout output.

| Task | Log Group |
|---|---|
| `dbt-snapshot` | `/ecs/dbt-snapshot` |
| `dbt-run-crm` | `/ecs/dbt-run-crm` |
| `dbt-build-finance` | `/ecs/dbt-build-finance` |
| `dbt-build-mart-monthly-customer-usage` | `/ecs/dbt-build-mart-monthly-customer-usage` |
| `dbt-build-medium-priority` | `/ecs/dbt-build-medium-priority` |

## Detecting Missed Runs

Since EventBridge fires the task but doesn't guarantee dbt succeeded, use the `run_summary` table to detect gaps:

```sql
-- Find jobs that haven't logged a successful run in the expected window
select
    command,
    max(completed_at) as last_success,
    now() - max(completed_at) as time_since_last_run
from dbt_logs.run_summary
where nodes_error = 0
group by command
order by time_since_last_run desc;
```

## Common Warnings

| Message | Meaning | Action |
|---|---|---|
| `Unable to do partial parsing because saved manifest not found` | No cached manifest in the container (expected in ECS) | None — harmless |
| `failed to log results to Postgres: permission denied` | `dbt` user lacks privileges on `dbt_logs` schema | Grant `CREATE` on database and `ALL ON ALL SEQUENCES IN SCHEMA dbt_logs` to `dbt` |