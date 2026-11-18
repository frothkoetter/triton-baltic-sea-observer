"""Resolve project root when CML runs a job as python3 script (not a notebook cell)."""
from __future__ import annotations

import os
import sys
from pathlib import Path


def resolve_job_root(*marker_files: str) -> Path:
    if not marker_files:
        marker_files = ("run_nba.py",)

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
                base / "cai-workbench" / "next-best-action",
                base / "next-best-action",
                base,
            ]
        )
    home = Path.home()
    candidates.extend(
        [
            home / "cai-workbench" / "next-best-action",
            home,
            Path("/home/cdsw") / "cai-workbench" / "next-best-action",
            Path("/home/cdsw"),
        ]
    )
    here = Path.cwd()
    candidates.extend([here, here / "next-best-action"])
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
        "Cannot locate next-best-action job root. In CML create a **Scheduled Job** "
        "(not Run notebook): Working directory = cai-workbench/next-best-action, "
        "command = python3 run_nba.py --no-demo --exit-if-quiet --persist-trino. "
        f"CDSW_WORKING_DIRECTORY={work or '(unset)'} cwd={here}"
    )


def install_job_root(root: Path) -> Path:
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    os.chdir(root)
    return root


def exit_job(rc: int) -> None:
    in_kernel = (
        "ipykernel" in sys.modules
        or "IPython" in sys.modules
        or (bool(sys.argv) and "ipykernel" in sys.argv[0])
    )
    if in_kernel and rc == 0:
        return
    raise SystemExit(rc)