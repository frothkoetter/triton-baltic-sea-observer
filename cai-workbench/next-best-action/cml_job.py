#!/usr/bin/env python3
# UC2 NBA — CML Scheduled Job: select this .py file.
# Env: NBA_PERSIST_TRINO, NBA_PHASE, NBA_DEMO_GREIFSWALD=0, TRINO_* (NBA_EXIT_IF_QUIET defaults 1)
# If NBA_MATERIALIZE=1: once apply cdw-analyse/sql/nba_apply_all_ddl.sh (includes phase6 table)
# Build: pip install -r requirements.txt
from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path


def _package_root() -> Path:
    if globals().get("__file__"):
        r = Path(__file__).resolve().parent
        if (r / "run_nba.py").is_file():
            return r
    for c in (
        Path("/home/cdsw/cai-workbench/next-best-action"),
        Path.home() / "cai-workbench" / "next-best-action",
        Path.cwd(),
        Path.cwd() / "cai-workbench" / "next-best-action",
    ):
        if (c / "run_nba.py").is_file():
            return c.resolve()
    raise RuntimeError(
        "next-best-action not found — job script must be cai-workbench/next-best-action/cml_job.py"
    )


def _load_run_nba(root: Path):
    path = root / "run_nba.py"
    spec = importlib.util.spec_from_file_location("run_nba", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["run_nba"] = mod
    spec.loader.exec_module(mod)
    return mod


def _in_jupyter_kernel() -> bool:
    return (
        "ipykernel" in sys.modules
        or "IPython" in sys.modules
        or (bool(sys.argv) and "ipykernel" in sys.argv[0])
    )


def _run() -> int:
    # CML scheduled runs: idle hour (no new alert) is success unless explicitly disabled.
    os.environ.setdefault("NBA_EXIT_IF_QUIET", "1")

    root = _package_root()
    os.chdir(root)
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    mod = _load_run_nba(root)
    rc = mod.main()
    if _in_jupyter_kernel() and rc in (0, 2):
        if rc == 2:
            print(
                "NBA idle (no actionable alert); Jupyter kernel — treated as success. "
                "Set NBA_EXIT_IF_QUIET=1 (default in cml_job.py) or use Scheduled Job engine.",
                flush=True,
            )
            return 0
        return 0
    mod._exit_job(rc)
    return rc


if __name__ == "__main__":
    _run()