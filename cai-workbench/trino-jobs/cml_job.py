#!/usr/bin/env python3
# CML Scheduled Job — run Trino SQL from cdw-analyse (outlier pipeline or lagebild view refresh).
# Env: TRINO_USER, TRINO_PASSWORD (or TRINO_JDBC_URL), TRINO_JOB=outliers|socmint_sketch|lagebild_views
# Optional: LAGEBILD_TRINO_* / TRINO_HOST / TRINO_CATALOG / TRINO_SCHEMA
from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path


def _package_root() -> Path:
    if globals().get("__file__"):
        r = Path(__file__).resolve().parent
        if (r / "run_trino_job.py").is_file():
            return r
    for c in (
        Path("/home/cdsw/cai-workbench/trino-jobs"),
        Path.home() / "cai-workbench" / "trino-jobs",
        Path.cwd() / "cai-workbench" / "trino-jobs",
    ):
        if (c / "run_trino_job.py").is_file():
            return c.resolve()
    raise RuntimeError(
        "trino-jobs not found — job script must be cai-workbench/trino-jobs/cml_job.py"
    )


def _load_runner(root: Path):
    path = root / "run_trino_job.py"
    spec = importlib.util.spec_from_file_location("run_trino_job", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _sync_trino_env_from_lagebild() -> None:
    """Copy LAGEBILD_TRINO_* into TRINO_* when the shared CML connection uses LAGEBILD prefix."""
    mapping = {
        "LAGEBILD_TRINO_USER": "TRINO_USER",
        "LAGEBILD_TRINO_PASSWORD": "TRINO_PASSWORD",
        "LAGEBILD_TRINO_JDBC_URL": "TRINO_JDBC_URL",
        "LAGEBILD_TRINO_HOST": "TRINO_HOST",
        "LAGEBILD_TRINO_PORT": "TRINO_PORT",
        "LAGEBILD_TRINO_CATALOG": "TRINO_CATALOG",
        "LAGEBILD_TRINO_SCHEMA": "TRINO_SCHEMA",
    }
    for src, dst in mapping.items():
        val = os.environ.get(src, "").strip()
        if val and not os.environ.get(dst, "").strip():
            os.environ[dst] = val


def _in_jupyter_kernel() -> bool:
    return (
        "ipykernel" in sys.modules
        or "IPython" in sys.modules
        or (bool(sys.argv) and "ipykernel" in sys.argv[0])
    )


def _exit_job(rc: int) -> None:
    """CML Jupyter kernel marks Engine failed on any SystemExit, even rc=0."""
    if _in_jupyter_kernel() and rc == 0:
        return
    raise SystemExit(rc)


def _run() -> int:
    root = _package_root()
    os.chdir(root)
    _sync_trino_env_from_lagebild()
    mod = _load_runner(root)
    return int(mod.main())


if __name__ == "__main__":
    _exit_job(_run())