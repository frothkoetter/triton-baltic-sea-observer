#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
export NVM_DIR="${NVM_DIR:-$ROOT/.nvm}"

if [[ -s "$NVM_DIR/nvm.sh" ]]; then
  # shellcheck source=/dev/null
  source "$NVM_DIR/nvm.sh"
  nvm use 20 2>/dev/null || nvm use default 2>/dev/null || true
fi

PORT="${CDSW_APP_PORT:-${API_PORT:-8090}}"
export CDSW_APP_PORT="$PORT"
export API_PORT="$PORT"
export NODE_ENV="${NODE_ENV:-production}"

if ! command -v node >/dev/null 2>&1; then
  echo "node not found — run: bash cdsw-build.sh" >&2
  exit 1
fi

if [[ ! -f "$ROOT/dist/index.html" ]]; then
  echo "dist/ missing — run: bash cdsw-build.sh" >&2
  exit 1
fi

exec node "$ROOT/server/index.js"