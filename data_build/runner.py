"""
Entry point for running dbt commands from ECS tasks.

ECS task definitions pass the dbt subcommand + args via the container
`command` override, e.g.: ["run", "--select", "crm_*"]

Usage (local):
    python -m data_build.runner run
    python -m data_build.runner run --select crm_contacts
    python -m data_build.runner snapshot
    python -m data_build.runner test
"""

import os
import subprocess
import sys
from pathlib import Path

from data_build.log_results import push

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt_analytics"
RUN_RESULTS_PATH = DBT_PROJECT_DIR / "target" / "run_results.json"


def main() -> None:
    args = sys.argv[1:]
    if not args:
        print("Usage: runner.py <dbt-subcommand> [args...]", file=sys.stderr)
        sys.exit(1)

    dbt_cmd = ["dbt"] + args
    print(f"[runner] Running: {' '.join(dbt_cmd)}", flush=True)

    result = subprocess.run(dbt_cmd, cwd=str(DBT_PROJECT_DIR.resolve()))

    if RUN_RESULTS_PATH.exists():
        try:
            push(RUN_RESULTS_PATH)
        except Exception as e:
            print(f"[runner] Warning: failed to log results to Postgres: {e}", file=sys.stderr)

    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
