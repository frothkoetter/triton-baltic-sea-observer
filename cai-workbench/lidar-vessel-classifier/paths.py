"""Project paths — works for ``python3 train.py`` and Cloudera AI Python (IPython) sessions."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _is_project_root(directory: Path) -> bool:
    return (directory / "train.py").is_file() and (directory / "src" / "features.py").is_file()


def resolve_project_root() -> Path:
    """
    Locate lidar-vessel-classifier root without assuming the caller has ``__file__``.

    Cloudera AI "Python" sessions are IPython (prompts show ``Cell In[n]``); they are not
    the same as ``python3 train.py`` in a shell.
    """
    try:
        here = Path(__file__).resolve().parent
        if _is_project_root(here):
            return here
    except NameError:
        pass

    for key in ("LIDAR_PROJECT_ROOT", "CDSW_WORKING_DIRECTORY"):
        value = os.environ.get(key)
        if value:
            candidate = Path(value).expanduser().resolve()
            if _is_project_root(candidate):
                return candidate

    cwd = Path.cwd().resolve()
    for candidate in (cwd, *cwd.parents):
        if _is_project_root(candidate):
            return candidate

    raise RuntimeError(
        "Cannot find lidar-vessel-classifier project root. "
        "Set CDSW_WORKING_DIRECTORY or LIDAR_PROJECT_ROOT, cd into the project, "
        "or run: python3 train.py"
    )


ROOT = resolve_project_root()
SRC = ROOT / "src"


def install_import_path() -> Path:
    """Add project root to sys.path for ``from src...`` / ``from features...``."""
    root = resolve_project_root()
    s = str(root)
    if s not in sys.path:
        sys.path.insert(0, s)
    return root
