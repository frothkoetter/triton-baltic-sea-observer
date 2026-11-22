#!/usr/bin/env python3
"""Refresh defense.lagebild views — runs cdw-analyse/run_lagebild_views.py from repo root."""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for base in (here.parents[2], here.parents[1], Path.cwd(), Path("/home/cdsw")):
        cdw = base / "cdw-analyse"
        if (cdw / "run_lagebild_views.py").is_file() and (
            cdw / "create_db_tables_trino.sql"
        ).is_file():
            return base.resolve()
    raise RuntimeError(
        "cdw-analyse not found — clone full repo at ~/ (need create_db_tables_trino.sql)"
    )


def _load_views_main(cdw: Path):
    script = cdw / "run_lagebild_views.py"
    marker = "drop statement-splitting semicolons"
    text = script.read_text(encoding="utf-8")
    if marker not in text:
        print(
            f"ERROR: {script} is outdated (missing view sanitizer).\n"
            "Fix: cd /home/cdsw && git fetch origin main && git checkout main && git pull origin main\n"
            "Then: grep 'drop statement-splitting' cdw-analyse/run_lagebild_views.py",
            file=sys.stderr,
        )
        raise SystemExit(2)
    if os.environ.get("TRINO_JOB_DEBUG"):
        print(f"lagebild_views_trino: using {script}", file=sys.stderr)
    spec = importlib.util.spec_from_file_location("run_lagebild_views", script)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {script}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["run_lagebild_views"] = mod
    if str(cdw) not in sys.path:
        sys.path.insert(0, str(cdw))
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    root = _repo_root()
    cdw = root / "cdw-analyse"
    os.chdir(cdw)
    mod = _load_views_main(cdw)
    return int(mod.main())


def _in_jupyter_kernel() -> bool:
    return (
        "ipykernel" in sys.modules
        or "IPython" in sys.modules
        or (bool(sys.argv) and "ipykernel" in sys.argv[0])
    )


def _exit_job(rc: int) -> None:
    if _in_jupyter_kernel() and rc == 0:
        return
    raise SystemExit(rc)


if __name__ == "__main__":
    _exit_job(main())