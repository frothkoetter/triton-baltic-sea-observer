"""Stdlib-only: put the project on sys.path (Cloudera AI Python sessions)."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _is_project_root(directory: Path) -> bool:
    return (directory / "train.py").is_file() and (directory / "src" / "features.py").is_file()


def ensure_project_on_sys_path() -> Path:
    """
    Use ``CDSW_WORKING_DIRECTORY`` / ``LIDAR_PROJECT_ROOT`` before any local imports.

    Safe to call from ``train.py``, ``deploy_cml_model.py``, or a session cell.
    """
    for key in ("CDSW_WORKING_DIRECTORY", "LIDAR_PROJECT_ROOT"):
        value = os.environ.get(key)
        if value:
            root = Path(value).expanduser().resolve()
            if _is_project_root(root):
                s = str(root)
                if s not in sys.path:
                    sys.path.insert(0, s)
                try:
                    os.chdir(root)
                except OSError:
                    pass
                return root

    try:
        root = Path(__file__).resolve().parent
        if _is_project_root(root):
            s = str(root)
            if s not in sys.path:
                sys.path.insert(0, s)
            return root
    except NameError:
        pass

    cwd = Path.cwd().resolve()
    for candidate in (cwd, *cwd.parents):
        if _is_project_root(candidate):
            s = str(candidate)
            if s not in sys.path:
                sys.path.insert(0, s)
            return candidate

    raise RuntimeError(
        "Project not on Python path. In one cell run:\n"
        "  import os, sys; sys.path.insert(0, os.environ['CDSW_WORKING_DIRECTORY'])\n"
        "  exec(open(os.environ['CDSW_WORKING_DIRECTORY']+'/deploy_now.py').read())"
    )
