#!/usr/bin/env bash
# Apply NBA + supplemental Trino DDL (same bundle as setup_cdw_trino_local.sh).
# Run from cdw-analyse with .env.trino.local sourced.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
ORDER="${ROOT}/sql/trino_supplemental_ddl.order"
PY="${PY:-python3}"
export PY
exec "${ROOT}/apply_supplemental_trino_ddl.sh"