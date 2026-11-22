#!/usr/bin/env bash
# Re-apply defense views only (after base tables exist). Requires .env.trino.local.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
ENV_FILE="${ROOT}/.env.trino.local"
[[ -f "$ENV_FILE" ]] || { echo "Missing $ENV_FILE" >&2; exit 1; }
set -a && source "$ENV_FILE" && set +a
export TRINO_ONLY_VIEWS=1
export TRINO_CONTINUE_ON_ERROR=1
PY="${ROOT}/.venv/bin/python"
[[ -x "$PY" ]] || PY=python3
exec "$PY" "${ROOT}/run_trino_ddl.py" "${ROOT}/create_db_tables_trino.sql" --continue