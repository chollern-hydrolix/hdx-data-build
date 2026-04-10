#!/bin/bash
# Source this script to load .env vars into your shell session:
#   source scripts/load_env.sh

set -a
source "$(dirname "${BASH_SOURCE[0]}")/../.env"
set +a

echo "Environment variables loaded from .env"