"""
Run once per notebook session (stdlib only — no prior project imports).

  %run /home/cdsw/lidar-vessel-classifier/notebook_bootstrap.py

Or set LIDAR_PROJECT_ROOT, then from that directory:

  %run notebook_bootstrap.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def find_project_root() -> Path:
    env = os.environ.get("LIDAR_PROJECT_ROOT")
    if env:
        root = Path(env).expanduser().resolve()
        if (root / "src" / "features.py").is_file():
            return root
        raise FileNotFoundError(f"LIDAR_PROJECT_ROOT has no src/features.py: {root}")

    for key in ("CDSW_WORKING_DIRECTORY", "PROJECT_PARENT_DIR"):
        if os.environ.get(key):
            root = Path(os.environ[key]).resolve()
            if (root / "src" / "features.py").is_file():
                return root
            nested = root / "cai-workbench" / "lidar-vessel-classifier"
            if (nested / "src" / "features.py").is_file():
                return nested

    cwd = Path.cwd().resolve()
    for candidate in (cwd, *cwd.parents):
        if (candidate / "src" / "features.py").is_file() and (candidate / "train.py").is_file():
            return candidate
        nested = candidate / "cai-workbench" / "lidar-vessel-classifier"
        if (nested / "src" / "features.py").is_file():
            return nested

    raise FileNotFoundError(
        "Could not find lidar-vessel-classifier. Set:\n"
        "  os.environ['LIDAR_PROJECT_ROOT'] = '/home/cdsw/.../lidar-vessel-classifier'\n"
        "then re-run this script."
    )


ROOT = find_project_root()
for p in (ROOT, ROOT / "src"):
    s = str(p)
    if s not in sys.path:
        sys.path.insert(0, s)

os.environ.setdefault("LIDAR_PROJECT_ROOT", str(ROOT))
print(f"LIDAR project ROOT = {ROOT}")
print("You can now: from features import extract_features_from_las_bytes")
