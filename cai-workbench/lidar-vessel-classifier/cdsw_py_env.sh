#!/usr/bin/env bash
# Source from a CML session terminal if imports fail:
#   source cdsw_py_env.sh
ROOT="${LIDAR_PROJECT_ROOT:-${CDSW_WORKING_DIRECTORY:-$(pwd)}}"
export LIDAR_PROJECT_ROOT="$ROOT"
export PYTHONPATH="${ROOT}/src:${ROOT}:${PYTHONPATH:-}"
echo "LIDAR_PROJECT_ROOT=$LIDAR_PROJECT_ROOT"
echo "PYTHONPATH includes project src/"
