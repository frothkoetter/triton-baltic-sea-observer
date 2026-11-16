#!/usr/bin/env bash
# Execute outlier_detection_trino.sql on CDW Trino (iceberg.defense).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

if [[ -f "${ROOT}/.env.trino.local" ]]; then
  # shellcheck disable=SC1090
  set -a
  source "${ROOT}/.env.trino.local"
  set +a
fi

export TRINO_CONTINUE_ON_ERROR="${TRINO_CONTINUE_ON_ERROR:-1}"
PY="python3"
if [[ -x "${ROOT}/.venv/bin/python" ]]; then
  PY="${ROOT}/.venv/bin/python"
fi

if [[ -z "${TRINO_PASSWORD:-}" ]]; then
  echo "Set TRINO_PASSWORD (e.g. in .env.trino.local)" >&2
  exit 1
fi

echo "==> Outlier detection pipeline"
TRINO_CONTINUE_ON_ERROR=1 "$PY" "${ROOT}/run_trino_ddl.py" "${ROOT}/outlier_detection_trino.sql" --continue
echo "Done. Query: SELECT Outlier_Type, COUNT(*) FROM vessel_outliers GROUP BY 1;"