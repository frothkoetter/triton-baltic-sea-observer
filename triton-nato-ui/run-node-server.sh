#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
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
  return 1
}

if [[ -z "${CDSW_APP_PORT:-}" && -z "${CDSW_READONLY_PORT:-}" ]]; then
  PORT="${API_PORT:-8090}"
else
  PORT="${CDSW_APP_PORT:-$CDSW_READONLY_PORT}"
fi
export CDSW_APP_PORT="$PORT"
export API_PORT="$PORT"
export NODE_ENV="${NODE_ENV:-production}"

if ! NODE_BIN="$(resolve_node)"; then
  echo "FATAL: node not found. In a Session run: cd $ROOT && bash cdsw-build.sh" >&2
  echo "  checked: .node-bin .node-runtime .nvm PATH=$PATH" >&2
  ls -la "$ROOT/.node-runtime/bin" 2>&1 || true
  ls -la "$NVM_DIR" 2>&1 | head -5 || true
  exit 1
fi

if [[ ! -f "$ROOT/dist/index.html" ]]; then
  echo "dist/ missing — run: bash cdsw-build.sh" >&2
  exit 1
fi

echo "Node $("$NODE_BIN" -v) starting on CDSW port ${PORT} (bin=$NODE_BIN)"
exec "$NODE_BIN" "$ROOT/server/index.js"