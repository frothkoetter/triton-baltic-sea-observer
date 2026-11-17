#!/usr/bin/env bash
# Cloudera AI Application entry: serve SPA + API (set TRINO_* in app env).
set -euo pipefail
cd "$(dirname "$0")"
export NODE_ENV=production
export API_PORT="${API_PORT:-8090}"
exec node server/index.js