"""Project paths. Import this module instead of copying ROOT = Path(__file__) into a REPL."""

from __future__ import annotations

import os
from pathlib import Path

# Always defined when this file is imported as a module.
ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"


def root_from_env() -> Path:
    """Project directory (for interactive sessions without __file__)."""
    return Path(os.environ.get("CDSW_WORKING_DIRECTORY", ROOT)).resolve()


def install_import_path() -> Path:
    """Add project root to sys.path so ``from src.features import ...`` works."""
    import sys

    root = root_from_env()
    s = str(root)
    if s not in sys.path:
        sys.path.insert(0, s)
    return root
