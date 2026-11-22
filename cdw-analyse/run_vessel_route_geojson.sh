#!/usr/bin/env bash
# Create vessel_ais_route_geojson view and print sample routes.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

if [[ -f "${ROOT}/.env.trino.local" ]]; then
  # shellcheck disable=SC1090
  set -a
  source "${ROOT}/.env.trino.local"
  set +a
fi

PY="python3"
[[ -x "${ROOT}/.venv/bin/python" ]] && PY="${ROOT}/.venv/bin/python"

if [[ -z "${TRINO_PASSWORD:-}" ]]; then
  echo "Set TRINO_PASSWORD in .env.trino.local" >&2
  exit 1
fi

echo "==> Create view vessel_ais_route_geojson"
TRINO_CONTINUE_ON_ERROR=0 "$PY" "${ROOT}/run_trino_ddl.py" "${ROOT}/vessel_route_geojson.sql"

MMSI="${1:-311040004}"
export SAMPLE_MMSI="$MMSI"
echo "==> Sample route MMSI=${MMSI}"
"$PY" <<'PY'
import os, json, trino
from trino.auth import BasicAuthentication
from trino_endpoint import apply_all_trino_env
apply_all_trino_env()
mmsi = int(os.environ["SAMPLE_MMSI"])
conn = trino.dbapi.connect(
    host=os.environ["TRINO_HOST"], port=int(os.environ.get("TRINO_PORT", "443")),
    user=os.environ["TRINO_USER"], catalog="iceberg", schema="defense", http_scheme="https",
    auth=BasicAuthentication(os.environ["TRINO_USER"], os.environ["TRINO_PASSWORD"]),
)
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM vessel_ais_route_geojson")
print("routes in view (24h):", cur.fetchone()[0])
cur.execute(
    f"""SELECT route_type, mmsi, start_timestamp, end_timestamp, start_port, end_port,
               point_count, cast(journey_duration as varchar), journey_duration_seconds, geo_json
        FROM vessel_ais_route_geojson WHERE mmsi = {mmsi}"""
)
row = cur.fetchone()
if row:
    keys = [
        "route_type", "mmsi", "start_timestamp", "end_timestamp", "start_port", "end_port",
        "point_count", "journey_duration", "journey_duration_seconds",
    ]
    print(json.dumps(dict(zip(keys, row[:-1])), indent=2, default=str))
    geom = json.loads(row[-1])
    print("coordinates:", len(geom["coordinates"]))
else:
    print("No route for MMSI")
conn.close()
PY