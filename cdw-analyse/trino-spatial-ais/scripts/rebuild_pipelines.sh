#!/usr/bin/env bash
# GeoJSON staging + segmentierte Pipeline-Tabelle neu aufbauen (sql/04 + sql/05)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CDW="$(cd "$ROOT/.." && pwd)"

export TRINO_CATALOG="${TRINO_CATALOG:-iceberg}"
export TRINO_SCHEMA="${TRINO_SCHEMA:-defense}"

if [[ -z "${TRINO_HOST:-}" || -z "${TRINO_USER:-}" ]]; then
  echo "Set TRINO_HOST and TRINO_USER" >&2
  exit 1
fi

for f in "$ROOT/sql/04_geojson_staging.sql" "$ROOT/sql/05_ostsee_pipelines_segmented.sql"; do
  echo "==> $f"
  python3 "$CDW/run_trino_ddl.py" "$f"
done

echo "ostsee_pipelines + ostsee_pipelines_segmented rebuilt."
