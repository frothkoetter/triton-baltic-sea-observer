#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CDW="$(cd "$ROOT/.." && pwd)"

export TRINO_CATALOG="${TRINO_CATALOG:-iceberg}"
export TRINO_SCHEMA="${TRINO_SCHEMA:-defense}"

if [[ -z "${TRINO_HOST:-}" || -z "${TRINO_USER:-}" ]]; then
  echo "Set TRINO_HOST, TRINO_USER (and TRINO_PASSWORD if needed), then re-run." >&2
  exit 1
fi

run_file() {
  local f="$1"
  echo "==> $f"
  PYTHON="${ROOT}/.venv/bin/python"
  if [[ ! -x "$PYTHON" ]]; then
  python3 -m pip install -q trino 2>/dev/null || true
    PYTHON=python3
  fi
  "$PYTHON" "$CDW/run_trino_ddl.py" "$f"
}

run_file "$ROOT/sql/01_schema_spatial_core.sql"
run_file "$ROOT/sql/02_view_area_violation.sql"
run_file "$ROOT/sql/03_ostsee_infrastruktur.sql"
run_file "$ROOT/sql/03a_refresh_ais_daten_view.sql"
run_file "$ROOT/sql/04_geojson_staging.sql"
run_file "$ROOT/sql/05_ostsee_pipelines_segmented.sql"
echo "Spatial lab applied (incl. GeoJSON staging + segmented pipelines) under ${TRINO_CATALOG}.${TRINO_SCHEMA}"
