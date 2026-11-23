#!/usr/bin/env bash
# SOCMINT trust upgrade via baltic_threat_tile_sketches (run after outlier_detection_trino.sql).
set -euo pipefail
cd "$(dirname "$0")"
if [[ -f .env.trino.local ]]; then
  set -a && source .env.trino.local && set +a
fi
export TRINO_CONTINUE_ON_ERROR="${TRINO_CONTINUE_ON_ERROR:-1}"
exec python3 run_trino_ddl.py socmint_sketch_verification_trino.sql --continue