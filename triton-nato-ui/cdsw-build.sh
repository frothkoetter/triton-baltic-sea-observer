#!/usr/bin/env bash
# Cloudera AI: install Node (if missing), npm deps, production build.
set -euo pipefail
cd "$(dirname "$0")"
export NVM_DIR="${NVM_DIR:-$(pwd)/.nvm}"
mkdir -p "$NVM_DIR"

install_node_nvm() {
  export NVM_DIR
  if [[ ! -s "$NVM_DIR/nvm.sh" ]]; then
    echo "Installing nvm …"
    curl -fsSL https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash
  fi
  # shellcheck source=/dev/null
  source "$NVM_DIR/nvm.sh"
  nvm install 20
  nvm use 20
}

install_node_tarball() {
  local ver="v20.18.0"
  local dir="$HOME/node-${ver}-linux-x64"
  if [[ ! -x "$dir/bin/node" ]]; then
    echo "Downloading Node ${ver} tarball …"
    curl -fsSL "https://nodejs.org/dist/${ver}/node-${ver}-linux-x64.tar.xz" \
      | tar -xJ -C "$HOME"
  fi
  export PATH="$dir/bin:$PATH"
}

if ! command -v npm >/dev/null 2>&1; then
  if command -v conda >/dev/null 2>&1; then
    echo "Installing nodejs via conda-forge …"
    conda install -y -c conda-forge nodejs=20
  elif command -v curl >/dev/null 2>&1; then
    install_node_nvm || install_node_tarball
  else
    echo "ERROR: npm not found; install Node 20+ or enable curl for bootstrap."
    exit 1
  fi
fi

npm ci 2>/dev/null || npm install
npm run build
echo "Build OK: dist/ + server/ (node $(node -v), npm $(npm -v))"