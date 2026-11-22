#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
pip install -r requirements.txt

if [[ -f bundled/model.pkl ]]; then
  cp -f bundled/model.pkl model.pkl
  cp -f bundled/label_names.json label_names.json
  echo "Using bundled/model.pkl"
elif [[ -f model.pkl ]]; then
  echo "Using existing model.pkl"
else
  echo "bundled/ not in build context — training deploy default model (fast synthetic)"
  python3 scripts/build_model_for_deploy.py
fi

ls -la model.pkl
echo "das-cable-classifier build OK — model.pkl ready for CML deploy"