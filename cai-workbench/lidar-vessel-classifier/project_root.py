"""Resolve lidar-vessel-classifier root (shell, job, or IPython ``Cell In[n]``)."""

from __future__ import annotations

from pathlib import Path

from cml_bootstrap import ensure_project_on_sys_path


def resolve_project_root() -> Path:
    return ensure_project_on_sys_path()
