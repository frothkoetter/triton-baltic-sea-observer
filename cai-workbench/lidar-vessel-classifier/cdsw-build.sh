#!/usr/bin/env bash
# Cloudera AI Workbench project build (training environment)
set -euo pipefail
pip install --no-cache-dir -r requirements-train.txt
pip install --no-cache-dir -r requirements.txt || true

# Every Python session: project root on sys.path (fixes Cell In[n] imports).
STARTUP="${HOME}/.ipython/profile_default/startup/00-lidar-vessel-classifier-path.py"
mkdir -p "$(dirname "${STARTUP}")"
cat > "${STARTUP}" <<'PY'
import os
import sys
from pathlib import Path

_root = os.environ.get("CDSW_WORKING_DIRECTORY") or os.environ.get("LIDAR_PROJECT_ROOT")
if _root:
    _p = Path(_root).expanduser().resolve()
    if (_p / "train.py").is_file():
        _s = str(_p)
        if _s not in sys.path:
            sys.path.insert(0, _s)
        _src = str(_p / "src")
        if _src not in sys.path:
            sys.path.insert(0, _src)
PY
echo "Wrote IPython startup: ${STARTUP}"
