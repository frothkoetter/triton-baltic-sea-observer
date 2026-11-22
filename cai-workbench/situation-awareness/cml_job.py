#!/usr/bin/env python3
# UC1 hourly Lagebild — CML Scheduled Job: select this .py file.
# Config via env: LAGEBILD_TRINO_*, LAGEBILD_LLM_*, LAGEBILD_S3_*, LAGEBILD_SKIP_LLM, LAGEBILD_INSERT_SQL
# Build once: pip install -r requirements.txt
from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path


def _package_root() -> Path:
    if globals().get("__file__"):
        r = Path(__file__).resolve().parent
        if (r / "run_hourly.py").is_file():
            return r
    for c in (
        Path("/home/cdsw/cai-workbench/situation-awareness"),
        Path.home() / "cai-workbench" / "situation-awareness",
        Path.cwd(),
        Path.cwd() / "cai-workbench" / "situation-awareness",
    ):
        if (c / "run_hourly.py").is_file():
            return c.resolve()
    raise RuntimeError(
        "situation-awareness not found — job script must be cai-workbench/situation-awareness/cml_job.py"
    )


_DEP_CHECKS = (
    ("fpdf", "fpdf2"),
    ("pandas", "pandas"),
    ("trino", "trino"),
    ("matplotlib", "matplotlib"),
)


def _missing_deps() -> list[str]:
    missing: list[str] = []
    for mod, pip_name in _DEP_CHECKS:
        try:
            __import__(mod)
        except ImportError:
            missing.append(pip_name)
    return missing


def _auto_pip_enabled() -> bool:
    return os.environ.get("LAGEBILD_AUTO_PIP", "1").strip().lower() not in (
        "0",
        "false",
        "no",
    )


def _verify_deps(root: Path) -> None:
    """fpdf2 imports as ``fpdf`` — install via requirements.txt on fresh CML engines."""
    missing = _missing_deps()
    if not missing:
        return
    if _auto_pip_enabled():
        import subprocess

        req = root / "requirements.txt"
        print(f"UC1: installing missing {missing} from {req}", flush=True)
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-q", "-r", str(req)],
        )
        missing = _missing_deps()
    if missing:
        raise RuntimeError(
            "UC1 missing Python packages: "
            + ", ".join(missing)
            + f". Run once: cd {root} && pip install -r requirements.txt"
            + " (or set LAGEBILD_AUTO_PIP=1, default)"
        )


def _load_run_hourly(root: Path):
    path = root / "run_hourly.py"
    spec = importlib.util.spec_from_file_location("run_hourly", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["run_hourly"] = mod
    spec.loader.exec_module(mod)
    return mod


def _run() -> int:
    root = _package_root()
    os.chdir(root)
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    _verify_deps(root)
    mod = _load_run_hourly(root)
    rc = mod.main()
    mod._exit_job(rc)
    return rc


if __name__ == "__main__":
    _run()