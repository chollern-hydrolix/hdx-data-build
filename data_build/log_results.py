"""
Reads dbt's run_results.json and inserts a summary + per-node rows into Postgres.

Tables (created automatically if they don't exist):
  dbt_logs.run_summary   — one row per invocation
  dbt_logs.run_node      — one row per model/test/snapshot
"""

import json
import os
import psycopg2
from pathlib import Path


def _connect():
    return psycopg2.connect(
        host=os.environ['DBT_HOST'],
        user=os.environ['DBT_USER'],
        password=os.environ['DBT_PASSWORD'],
        dbname=os.environ.get('DBT_DATABASE', 'postgres'),
        port=int(os.environ.get('DBT_PORT', 5432)),
    )


def _ensure_tables(cur):
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS dbt_logs;

        CREATE TABLE IF NOT EXISTS dbt_logs.run_summary (
            invocation_id     TEXT PRIMARY KEY,
            dbt_version       TEXT,
            target            TEXT,
            command           TEXT,
            started_at        TIMESTAMPTZ,
            completed_at      TIMESTAMPTZ,
            elapsed_seconds   NUMERIC,
            total_nodes       INT,
            nodes_success     INT,
            nodes_error       INT,
            nodes_skip        INT,
            nodes_warn        INT,
            logged_at         TIMESTAMPTZ DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS dbt_logs.run_node (
            id                BIGSERIAL PRIMARY KEY,
            invocation_id     TEXT REFERENCES dbt_logs.run_summary(invocation_id),
            unique_id         TEXT,
            node_type         TEXT,
            status            TEXT,
            execution_seconds NUMERIC,
            failures          INT,
            message           TEXT,
            logged_at         TIMESTAMPTZ DEFAULT now()
        );
    """)


def push(run_results_path: Path):
    data = json.loads(run_results_path.read_text())

    meta = data["metadata"]
    invocation_id = meta["invocation_id"]
    dbt_version = meta["dbt_version"]
    started_at = meta.get("invocation_started_at") or meta["generated_at"]
    completed_at = meta["generated_at"]
    elapsed = data.get("elapsed_time")
    args = data.get("args", {})
    target = args.get("target", "unknown")
    command = args.get("invocation_command", "unknown")

    results = data.get("results", [])
    status_counts = {"success": 0, "pass": 0, "error": 0, "skipped": 0, "warn": 0, "fail": 0}
    for r in results:
        s = r.get("status", "")
        if s in status_counts:
            status_counts[s] += 1

    nodes_success = status_counts["success"] + status_counts["pass"]
    nodes_error = status_counts["error"] + status_counts["fail"]
    nodes_skip = status_counts["skipped"]
    nodes_warn = status_counts["warn"]

    conn = _connect()
    try:
        with conn:
            with conn.cursor() as cur:
                # _ensure_tables(cur)

                cur.execute("""
                    INSERT INTO dbt_logs.run_summary (
                        invocation_id, dbt_version, target, command,
                        started_at, completed_at, elapsed_seconds,
                        total_nodes, nodes_success, nodes_error, nodes_skip, nodes_warn
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (invocation_id) DO NOTHING
                """, (
                    invocation_id, dbt_version, target, command,
                    started_at, completed_at, elapsed,
                    len(results), nodes_success, nodes_error, nodes_skip, nodes_warn,
                ))

                for r in results:
                    unique_id = r.get("unique_id", "")
                    node_type = unique_id.split(".")[0] if "." in unique_id else "unknown"
                    cur.execute("""
                        INSERT INTO dbt_logs.run_node (
                            invocation_id, unique_id, node_type,
                            status, execution_seconds, failures, message
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        invocation_id,
                        unique_id,
                        node_type,
                        r.get("status"),
                        r.get("execution_time"),
                        r.get("failures"),
                        r.get("message"),
                    ))
    finally:
        conn.close()

    print(f"[log_results] Logged invocation {invocation_id} — "
          f"{nodes_success} success, {nodes_error} error, {nodes_skip} skip")
