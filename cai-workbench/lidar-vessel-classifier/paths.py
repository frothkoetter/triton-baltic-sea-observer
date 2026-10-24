"""Project paths. Import this module instead of copying ROOT = Path(__file__) into a REPL."""

from __future__ import annotations

import os
from pathlib import Path

# Always defined when this file is imported as a module.
ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"


def root_from_env() -> Path:
    """Fallback when only CDSW_WORKING_DIRECTORY is known."""
    return Path(os.environ.get("CDSW_WORKING_DIRECTORY", ROOT)).resolve()
