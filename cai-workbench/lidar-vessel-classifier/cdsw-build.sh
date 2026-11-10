#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
pip install -r requirements.txt
if [[ ! -f model.pkl && -f bundled/model.pkl ]]; then
  cp bundled/model.pkl model.pkl
  cp bundled/label_names.json label_names.json
  echo "Installed bundled/model.pkl → model.pkl (skipped if you already trained)"
fi
