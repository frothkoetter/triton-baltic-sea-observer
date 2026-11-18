#!/usr/bin/env bash
# Run UC2 NBA job from a CML Session (any cwd). Does not rely on CDSW_WORKING_DIRECTORY.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
echo "UC2 root=$ROOT"
bash cdsw-build.sh
exec python3 run_nba.py "$@"