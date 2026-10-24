"""Project paths — works for ``python3 train.py`` and Cloudera AI Python (IPython) sessions."""

from __future__ import annotations

import sys
from pathlib import Path

from project_root import resolve_project_root

ROOT = resolve_project_root()
SRC = ROOT / "src"


def install_import_path() -> Path:
    """Add project root to sys.path for ``from src...`` / ``from features...``."""
    root = resolve_project_root()
    s = str(root)
    if s not in sys.path:
        sys.path.insert(0, s)
    return root
