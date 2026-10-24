#!/usr/bin/env bash
# Cloudera AI / CML job entrypoint for train.py
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

export LIDAR_S3_PREFIX="${LIDAR_S3_PREFIX:-s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/}"

exec python3 train.py --s3-prefix "$LIDAR_S3_PREFIX" "$@"
