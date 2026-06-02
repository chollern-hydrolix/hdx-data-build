"""
Entry point for running dbt commands from ECS tasks.

ECS task definitions pass the dbt subcommand + args via the container
`command` override, e.g.: ["run", "--select", "crm_*"]

Multiple sequential commands are separated by --then. Each command runs only
if the previous one succeeded, and results are logged to Postgres after each step.

Usage (local):
    python -m data_build.runner run
    python -m data_build.runner run --select crm_contacts
    python -m data_build.runner snapshot
    python -m data_build.runner snapshot --then build --select path:models/marts/crm_periodic/snapshots
    python -m data_build.runner test
"""

import subprocess
import sys
from pathlib import Path

from data_build.log_results import push

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt_analytics"
RUN_RESULTS_PATH = DBT_PROJECT_DIR / "target" / "run_results.json"


def _split_commands(args: list[str]) -> list[list[str]]:
    """Split args on '--then' into sequential command groups."""
    commands: list[list[str]] = []
    current: list[str] = []
    for arg in args:
        if arg == '--then':
            if current:
                commands.append(current)
            current = []
        else:
            current.append(arg)
    if current:
        commands.append(current)
    return commands


def _run_command(args: list[str]) -> int:
    dbt_cmd = ['dbt'] + args
    print(f'[runner] Running: {" ".join(dbt_cmd)}', flush=True)
    result = subprocess.run(dbt_cmd, cwd=str(DBT_PROJECT_DIR.resolve()))
    if RUN_RESULTS_PATH.exists():
        try:
            push(RUN_RESULTS_PATH)
        except Exception as e:
            print(f'[runner] Warning: failed to log results to Postgres: {e}', file=sys.stderr)
    return result.returncode


def main() -> None:
    args = sys.argv[1:]
    if not args:
        print('Usage: runner.py <dbt-subcommand> [args...] [--then <dbt-subcommand> [args...]]', file=sys.stderr)
        sys.exit(1)

    commands = _split_commands(args)
    for cmd_args in commands:
        returncode = _run_command(cmd_args)
        if returncode != 0:
            sys.exit(returncode)

    sys.exit(0)


if __name__ == '__main__':
    main()
