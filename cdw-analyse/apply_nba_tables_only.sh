#!/usr/bin/env bash
# Add missing NBA Iceberg tables/columns only (no DROP, no full defense DDL).
# Run from cdw-analyse after sourcing .env.trino.local.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
export PY="${PY:-python3}"
export TRINO_SKIP_DROP=1
exec "${ROOT}/apply_supplemental_trino_ddl.sh"