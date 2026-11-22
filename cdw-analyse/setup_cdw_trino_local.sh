#!/usr/bin/env bash
# Full CDW Trino DDL apply for local development (iceberg.defense).
#
# Usage:
#   cp .env.trino.local.example .env.trino.local   # once
#   # edit TRINO_PASSWORD in .env.trino.local
#   ./setup_cdw_trino_local.sh
#
# Or export TRINO_USER / TRINO_PASSWORD / TRINO_JDBC_URL and run without .env file.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

ENV_FILE="${ROOT}/.env.trino.local"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  set -a
  source "$ENV_FILE"
  set +a
elif [[ -f "${ROOT}/.env.trino.local.example" ]]; then
  echo "Missing ${ENV_FILE} — copy .env.trino.local.example and set TRINO_PASSWORD." >&2
  exit 1
fi

export TRINO_JDBC_URL="${TRINO_JDBC_URL:-jdbc:trino://cdw-triton.dw-triton-cdp-env.a465-9q4k.cloudera.site:443/iceberg/defense}"
export TRINO_USER="${TRINO_USER:-frothkoetter}"
export TRINO_CATALOG="${TRINO_CATALOG:-iceberg}"
export TRINO_SCHEMA="${TRINO_SCHEMA:-defense}"
export TRINO_PORT="${TRINO_PORT:-443}"
export TRINO_CONNECT_SCHEMA="${TRINO_CONNECT_SCHEMA:-defense}"
export TRINO_SKIP_CREATE_SCHEMA="${TRINO_SKIP_CREATE_SCHEMA:-1}"
export TRINO_QUALIFY_SCHEMA="${TRINO_QUALIFY_SCHEMA:-1}"
export TRINO_SKIP_DROP="${TRINO_SKIP_DROP:-1}"
export TRINO_FORGIVE_DROP_404="${TRINO_FORGIVE_DROP_404:-1}"

if [[ -z "${TRINO_PASSWORD:-}" ]]; then
  echo "Set TRINO_PASSWORD in .env.trino.local or the environment." >&2
  exit 1
fi

# Parse JDBC → TRINO_HOST (run_trino_ddl.py also accepts TRINO_JDBC_URL)
if [[ -z "${TRINO_HOST:-}" && -n "${TRINO_JDBC_URL}" ]]; then
  TRINO_HOST="$(sed -E 's|^jdbc:trino://([^:/]+).*|\1|' <<<"$TRINO_JDBC_URL")"
  export TRINO_HOST
fi

if grep -q '_skip_create_schema' "${ROOT}/run_trino_ddl.py" 2>/dev/null; then
  echo "ERROR: cdw-analyse/run_trino_ddl.py is outdated (USE iceberg.defense still runs → 404)." >&2
  echo "       From repo root: git pull origin main   (need fix >= 873d677; skips USE + qualifies defense.*)" >&2
  exit 1
fi

echo "==> Trino target: https://${TRINO_HOST}:${TRINO_PORT} catalog=${TRINO_CATALOG} schema=${TRINO_SCHEMA} user=${TRINO_USER}"
echo "    Session schema for DDL: ${TRINO_CONNECT_SCHEMA} (target: ${TRINO_SCHEMA})"
echo "    TRINO_SKIP_CREATE_SCHEMA=${TRINO_SKIP_CREATE_SCHEMA}  TRINO_QUALIFY_SCHEMA=${TRINO_QUALIFY_SCHEMA}"
echo "    TRINO_SKIP_DROP=${TRINO_SKIP_DROP}  (set 0 for full DROP+CREATE re-apply)"
echo "    (CREATE SCHEMA / USE via Trino often 404 on CDW — create iceberg.defense in DW UI, keep skip=1)"

VENV="${ROOT}/.venv"
PY="python3"
if [[ -x "${VENV}/bin/python" ]] && "${VENV}/bin/python" -c "import trino" 2>/dev/null; then
  PY="${VENV}/bin/python"
elif [[ ! -d "${VENV}/bin" ]] && python3 -m venv "$VENV" 2>/dev/null; then
  echo "==> Created venv ${VENV}"
  "${VENV}/bin/python" -m pip install -q -U pip
  "${VENV}/bin/python" -m pip install -q -r "${ROOT}/requirements-trino.txt"
  PY="${VENV}/bin/python"
else
  echo "==> Installing trino into user/python3 environment"
  python3 -m pip install -q -r "${ROOT}/requirements-trino.txt"
  rm -rf "$VENV" 2>/dev/null || true
fi
DDL="${ROOT}/create_db_tables_trino.sql"
NSB="${ROOT}/ships_nsb_trade_1000.sql"

run_ddl() {
  local file="$1"
  local cont="${2:-0}"
  echo ""
  echo "==> Applying $(basename "$file")"
  if [[ "$cont" == "1" ]]; then
    TRINO_CONTINUE_ON_ERROR=1 "$PY" "${ROOT}/run_trino_ddl.py" "$file" --continue
  else
    "$PY" "${ROOT}/run_trino_ddl.py" "$file"
  fi
}

if [[ -x "$PY" ]] || command -v python3 >/dev/null; then
  echo ""
  echo "==> CDW Trino doctor (optional connectivity checks)"
  "$PY" "${ROOT}/trino_cdw_doctor.py" || true
fi

run_ddl "$DDL" 0

if [[ -f "$NSB" ]]; then
  echo ""
  echo "==> Refresh NSB trade ship rows (311040001–311041000)"
  "$PY" - <<'PY'
import os
from run_trino_ddl import connect

conn = connect(for_ddl=False, schema=os.environ.get("TRINO_SCHEMA", "defense"))
cur = conn.cursor()
cur.execute(
    "DELETE FROM iceberg.defense.ships WHERE mmsi >= 311040001 AND mmsi <= 311041000"
)
conn.close()
print("DELETE NSB MMSI range: ok")
PY
  run_ddl "$NSB" 1
fi

echo ""
echo "==> Smoke checks"
"$PY" - <<'PY'
import os
from run_trino_ddl import connect

conn = connect(for_ddl=False, schema=os.environ.get("TRINO_SCHEMA", "defense"))
cur = conn.cursor()
for q in (
    "SELECT COUNT(*) FROM iceberg.defense.ships",
    "SELECT COUNT(*) FROM iceberg.defense.ais_events_ice",
    "SELECT COUNT(*) FROM iceberg.defense.ships WHERE mmsi >= 311040001",
):
    cur.execute(q)
    row = cur.fetchone()
    print(f"  {q} -> {row[0]}")
conn.close()
PY

echo ""
echo ""
echo "Optional: ./run_outlier_detection_trino.sh (see README_outlier_detection_trino.md)"
echo "Done. JDBC: ${TRINO_JDBC_URL}"
echo "CLI: trino --server https://${TRINO_HOST}:${TRINO_PORT} --user ${TRINO_USER} --password --catalog ${TRINO_CATALOG} --schema ${TRINO_SCHEMA}"