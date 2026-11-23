#!/usr/bin/env bash
# CML Session: Node is not on PATH — use same resolution as run-node-server.sh / cdsw-build.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export NVM_DIR="${NVM_DIR:-$ROOT/.nvm}"

resolve_node() {
  if [[ -f "$ROOT/.node-bin" ]]; then
    local saved
    saved="$(tr -d '\n' < "$ROOT/.node-bin")"
    if [[ -x "$saved" ]]; then
      echo "$saved"
      return
    fi
  fi
  if [[ -x "$ROOT/.node-runtime/bin/node" ]]; then
    echo "$ROOT/.node-runtime/bin/node"
    return
  fi
  if [[ -s "$NVM_DIR/nvm.sh" ]]; then
    # shellcheck source=/dev/null
    source "$NVM_DIR/nvm.sh"
    nvm use 20 2>/dev/null || nvm use default 2>/dev/null || true
    if command -v node >/dev/null 2>&1; then
      command -v node
      return
    fi
  fi
  if command -v node >/dev/null 2>&1; then
    command -v node
    return
  fi
  echo "ERROR: node not found. Run: bash cdsw-build.sh" >&2
  return 1
}

NODE="$(resolve_node)"
[[ -f scripts/ttyd-e2e.mjs ]] || { echo "Missing scripts/ttyd-e2e.mjs — git pull main" >&2; exit 1; }
[[ -f .env.trino.local ]] 2>/dev/null && set -a && source ../cdw-analyse/.env.trino.local 2>/dev/null || true
set -a
[[ -f ../cdw-analyse/.env.trino.local ]] && source ../cdw-analyse/.env.trino.local
[[ -f .env.trino.local ]] && source .env.trino.local
set +a
export TRINO_SCHEMA="${TRINO_SCHEMA:-defense}"
export TRINO_CONNECT_SCHEMA="${TRINO_CONNECT_SCHEMA:-defense}"
export LAGEBILD_DATA_ENGINE="${LAGEBILD_DATA_ENGINE:-auto}"

exec "$NODE" scripts/ttyd-e2e.mjs "$@"