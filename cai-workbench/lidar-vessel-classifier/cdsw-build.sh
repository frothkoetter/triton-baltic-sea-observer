#!/usr/bin/env bash
# Cloudera AI Workbench project build (training environment)
set -euo pipefail
pip install --no-cache-dir -r requirements-train.txt
pip install --no-cache-dir -r requirements.txt || true
