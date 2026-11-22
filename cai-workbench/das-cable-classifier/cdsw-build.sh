#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
pip install -r requirements.txt
if [[ ! -f bundled/model.pkl ]]; then
  echo "ERROR: bundled/model.pkl missing (git checkout required)" >&2
  exit 1
fi
cp -f bundled/model.pkl model.pkl
cp -f bundled/label_names.json label_names.json
ls -la model.pkl bundled/model.pkl
echo "das-cable-classifier build OK — model.pkl ready for CML deploy"