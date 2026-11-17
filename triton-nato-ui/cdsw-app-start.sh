#!/usr/bin/env bash
# Cloudera AI Application entry: serve SPA + API (set TRINO_* in app env).
set -euo pipefail
cd "$(dirname "$0")"

if ! command -v node >/dev/null 2>&1; then
  export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
  if [[ -s "$NVM_DIR/nvm.sh" ]]; then
    # shellcheck source=/dev/null
    source "$NVM_DIR/nvm.sh"
    nvm use 20 2>/dev/null || true
  fi
  for dir in "$HOME"/node-v20.*-linux-x64; do
    if [[ -x "$dir/bin/node" ]]; then
      export PATH="$dir/bin:$PATH"
      break
    fi
  done
fi

if ! command -v node >/dev/null 2>&1; then
  echo "ERROR: node not in PATH; run bash cdsw-build.sh in the Application build step first."
  exit 1
fi

export NODE_ENV=production
export API_PORT="${API_PORT:-8090}"
exec node server/index.js