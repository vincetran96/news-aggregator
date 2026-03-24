#!/usr/bin/env bash
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

set -a
source "$PROJECT_DIR/build/.env"
source "$PROJECT_DIR/.venv/bin/activate"
set +a

cd "$PROJECT_DIR"
exec uv run dagster dev -m dagster_src -h 0.0.0.0 -p 43333
