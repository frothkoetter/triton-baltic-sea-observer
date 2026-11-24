#!/usr/bin/env bash
# CML: capture live Trino data for demo mode (same node resolution as ttyd-e2e).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export NVM_DIR="${NVM_DIR:-$ROOT/.nvm}"

resolve_node() {
  if [[ -f "$ROOT/.node-bin" ]]; then
    local saved
    saved="$(tr -d '\n' < "$ROOT/.node-bin")"
    [[ -x "$saved" ]] && echo "$saved" && return
  fi
  if [[ -x "$ROOT/.node-runtime/bin/node" ]]; then
    echo "$ROOT/.node-runtime/bin/node"
    return
  fi
  if [[ -s "$NVM_DIR/nvm.sh" ]]; then
    # shellcheck source=/dev/null
    source "$NVM_DIR/nvm.sh"
    nvm use 20 2>/dev/null || nvm use default 2>/dev/null || true
    command -v node && return
  fi
  command -v node
}

NODE="$(resolve_node)"
[[ -f ../cdw-analyse/.env.trino.local ]] && set -a && source ../cdw-analyse/.env.trino.local && set +a
exec "$NODE" scripts/capture-live-snapshot.mjs "$@"