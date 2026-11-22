#!/usr/bin/env bash
# NBA Iceberg tables, SAP staging, vessel_ais_route_geojson view, and view refresh.
# Requires TRINO_* env (e.g. .env.trino.local). Run after create_db_tables_trino.sql.
#
# Usage: cd cdw-analyse && ./apply_supplemental_trino_ddl.sh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

export TRINO_CATALOG="${TRINO_CATALOG:-iceberg}"
export TRINO_SCHEMA="${TRINO_SCHEMA:-defense}"
export TRINO_CONNECT_SCHEMA="${TRINO_CONNECT_SCHEMA:-defense}"
export TRINO_SKIP_CREATE_SCHEMA="${TRINO_SKIP_CREATE_SCHEMA:-1}"
export TRINO_QUALIFY_SCHEMA="${TRINO_QUALIFY_SCHEMA:-1}"
export TRINO_SKIP_DROP="${TRINO_SKIP_DROP:-1}"

ORDER="${ROOT}/sql/trino_supplemental_ddl.order"
PY="${PY:-python3}"
if [[ ! -f "$ORDER" ]]; then
  echo "Missing $ORDER" >&2
  exit 1
fi

run_ddl_continue() {
  local file="$1"
  echo ""
  echo "==> Applying $(basename "$file")"
  TRINO_CONTINUE_ON_ERROR=1 "$PY" "${ROOT}/run_trino_ddl.py" "$file" --continue
}

echo "==> Supplemental Trino DDL (NBA + extra views)"
while IFS= read -r line || [[ -n "$line" ]]; do
  line="${line%%#*}"
  line="$(echo "$line" | xargs)"
  [[ -z "$line" ]] && continue
  path="${ROOT}/${line}"
  if [[ ! -f "$path" ]]; then
    echo "WARN: missing DDL file $line — skip" >&2
    continue
  fi
  run_ddl_continue "$path"
done < "$ORDER"

echo ""
echo "==> Re-apply defense views (CREATE OR REPLACE; idempotent)"
export TRINO_ONLY_VIEWS=1
TRINO_CONTINUE_ON_ERROR=1 "$PY" "${ROOT}/run_trino_ddl.py" "${ROOT}/create_db_tables_trino.sql" --continue
unset TRINO_ONLY_VIEWS

echo ""
echo "Supplemental DDL complete."