#!/bin/bash
set -euo pipefail

cd /app

echo "[entrypoint] Installing dbt packages..."
dbt deps --quiet --project-dir dbt_analytics

echo "[entrypoint] Running: dbt $*"
exec python -m data_build.runner "$@"
