"""Resolve lidar-vessel-classifier root (shell, job, or IPython ``Cell In[n]``)."""

from __future__ import annotations

import os
from pathlib import Path


def _is_project_root(directory: Path) -> bool:
    return (directory / "train.py").is_file() and (directory / "src" / "features.py").is_file()


def resolve_project_root() -> Path:
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
        "Cannot find lidar-vessel-classifier root. "
        "Run: import os, sys; sys.path.insert(0, os.environ['CDSW_WORKING_DIRECTORY']); "
        "import deploy_cml_model; deploy_cml_model.main()"
    )
