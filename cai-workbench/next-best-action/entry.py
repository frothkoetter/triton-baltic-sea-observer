#!/usr/bin/env python3
"""CML Scheduled Job (UC2). Prefer: bash run-from-session.sh (not notebook cells)."""
from __future__ import annotations

import os
import sys
from pathlib import Path


def _root() -> Path:
    if globals().get("__file__"):
        r = Path(__file__).resolve().parent
        if (r / "run_nba.py").is_file():
            return r
    for c in (
        Path("/home/cdsw/cai-workbench/next-best-action"),
        Path.home() / "cai-workbench" / "next-best-action",
        Path.cwd(),
    ):
        if (c / "run_nba.py").is_file():
            return c.resolve()
    raise SystemExit(
        "Cannot find next-best-action. Use bash run-from-session.sh from that folder."
    )


ROOT = _root()
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from run_nba import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())