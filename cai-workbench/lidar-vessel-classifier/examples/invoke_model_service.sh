#!/usr/bin/env bash
# Test LiDAR classifier Model Service (same call NiFi InvokeHTTP should make).
# Usage:
#   export LIDAR_MODEL_URL='https://modelservice.ml-….cloudera.site/model'
#   export LIDAR_MODEL_ACCESS_KEY='…'
#   export LIDAR_S3_URI='s3a://goes-se-sandbox/data/user/…/file.las'
#   ./invoke_model_service.sh

set -euo pipefail

: "${LIDAR_MODEL_URL:?Set LIDAR_MODEL_URL (base URL through /model)}"
: "${LIDAR_MODEL_ACCESS_KEY:?Set LIDAR_MODEL_ACCESS_KEY}"
: "${LIDAR_S3_URI:?Set LIDAR_S3_URI}"

curl -sS -H "Content-Type: application/json" -X POST \
  "${LIDAR_MODEL_URL}?accessKey=${LIDAR_MODEL_ACCESS_KEY}" \
  -d "{\"request\":{\"s3_uri\":\"${LIDAR_S3_URI}\"}}"

echo
