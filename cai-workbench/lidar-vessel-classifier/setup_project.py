"""Put project src/ on sys.path (works in notebooks and scripts)."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def project_root() -> Path:
    try:
        return Path(__file__).resolve().parent
    except NameError:
        for key in ("LIDAR_PROJECT_ROOT", "CDSW_WORKING_DIRECTORY", "PROJECT_PARENT_DIR"):
            if os.environ.get(key):
                return Path(os.environ[key]).resolve()
        cwd = Path.cwd().resolve()
        for candidate in (cwd, *cwd.parents):
            if (candidate / "train.py").is_file() and (candidate / "src" / "features.py").is_file():
                return candidate
        return cwd


def ensure_src_path() -> Path:
    root = project_root()
    src = root / "src"
    if not (src / "features.py").is_file():
        raise FileNotFoundError(
            f"Cannot find src/features.py under {root}. "
            "cd to the project directory or set LIDAR_PROJECT_ROOT."
        )
    src_str = str(src)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)
    return root


if __name__ == "__main__":
    r = ensure_src_path()
    print(f"ROOT={r}")
    print(f"sys.path[0]={sys.path[0]}")
