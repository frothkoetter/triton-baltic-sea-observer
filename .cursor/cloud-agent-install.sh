#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../nifi-processors/dev"
python3 -m pip install --user -r requirements-dev.txt
