#!/usr/bin/env bash
# Cloudera AI: install Node (if missing), npm deps, production build.
set -euo pipefail
cd "$(dirname "$0")"

if ! command -v npm >/dev/null 2>&1; then
  if command -v conda >/dev/null 2>&1; then
    echo "Installing nodejs via conda-forge …"
    conda install -y -c conda-forge nodejs=20
  else
    echo "ERROR: npm not found and conda unavailable."
    echo "Use a Cloudera AI Application with Node runtime, or install Node 20+ on the host."
    exit 1
  fi
fi

npm ci 2>/dev/null || npm install
npm run build
echo "Build OK: dist/ + server/"