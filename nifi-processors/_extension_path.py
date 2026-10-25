"""Ensure sibling modules resolve when NiFi loads a single processor file."""

import sys
from pathlib import Path

_DIR = Path(__file__).resolve().parent
if _DIR.as_posix() not in sys.path:
    sys.path.insert(0, _DIR.as_posix())
