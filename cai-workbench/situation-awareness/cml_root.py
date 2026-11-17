"""Resolve project root when CML runs a job as python3 script (not a notebook cell)."""
from __future__ import annotations

import os
import sys
from pathlib import Path


def resolve_job_root(*marker_files: str) -> Path:
    """
    Locate this package directory.

    CML Scheduled Jobs must run ``python3 run_hourly.py`` (or ``entry.py``),
    not paste the file into a notebook — ``__file__`` is undefined in cells.
    """
    if not marker_files:
        marker_files = ("run_hourly.py",)

    mod = globals().get("__file__")
    if mod:
        root = Path(mod).resolve().parent
        if any((root / m).is_file() for m in marker_files):
            return root

    work = os.environ.get("CDSW_WORKING_DIRECTORY", "").strip()
    if work in ("", "/"):
        work = ""
    candidates: list[Path] = []
    if work:
        base = Path(work)
        candidates.extend(
            [
                base / "cai-workbench" / "situation-awareness",
                base / "situation-awareness",
                base,
            ]
        )
    home = Path.home()
    candidates.extend(
        [
            home / "cai-workbench" / "situation-awareness",
            home,
            Path("/home/cdsw") / "cai-workbench" / "situation-awareness",
            Path("/home/cdsw"),
        ]
    )
    here = Path.cwd()
    candidates.extend([here, here / "situation-awareness"])
    seen: set[Path] = set()
    for c in candidates:
        try:
            cr = c.resolve()
        except OSError:
            continue
        if cr in seen:
            continue
        seen.add(cr)
        if any((cr / m).is_file() for m in marker_files):
            return cr

    raise RuntimeError(
        "Cannot locate situation-awareness job root. In CML create a **Scheduled Job** "
        "(not Run notebook): Working directory = cai-workbench/situation-awareness, "
        "command = python3 run_hourly.py. "
        f"CDSW_WORKING_DIRECTORY={work or '(unset)'} cwd={here}"
    )


def install_job_root(root: Path) -> Path:
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    os.chdir(root)
    return root


def exit_job(rc: int) -> None:
    """
    Exit batch jobs with a status code.

    Jupyter/CML ipykernel treats any SystemExit as a failed engine (even code 0).
    On success in-kernel, return normally instead of raising SystemExit(0).
    """
    in_kernel = (
        "ipykernel" in sys.modules
        or "IPython" in sys.modules
        or (bool(sys.argv) and "ipykernel" in sys.argv[0])
    )
    if in_kernel and rc == 0:
        return
    raise SystemExit(rc)