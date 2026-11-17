#!/usr/bin/env python3
"""Cloudera AI Application entry — listen on CDSW_APP_PORT via Node/Express."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def resolve_root() -> Path:
    """Project root (triton-nato-ui). Works as script; not in Jupyter cells."""
    if globals().get("__file__"):
        return Path(__file__).resolve().parent
    work = os.environ.get("CDSW_WORKING_DIRECTORY", "").strip()
    if work:
        for candidate in (Path(work) / "triton-nato-ui", Path(work)):
            if (candidate / "server" / "index.js").is_file():
                return candidate
    here = Path.cwd()
    if (here / "server" / "index.js").is_file():
        return here
    nested = here / "triton-nato-ui"
    if (nested / "server" / "index.js").is_file():
        return nested
    raise RuntimeError(
        "Cannot locate triton-nato-ui. Run: python3 triton-nato-ui/entry.py "
        "(not from a notebook cell). CDSW_WORKING_DIRECTORY="
        f"{work or '(unset)'}"
    )


ROOT = resolve_root()
os.chdir(ROOT)
os.environ["NVM_DIR"] = str(ROOT / ".nvm")

port = os.environ.get("CDSW_APP_PORT") or os.environ.get("CDSW_READONLY_PORT") or ""
cwd = os.environ.get("CDSW_WORKING_DIRECTORY", os.getcwd())

print("=== triton-nato-ui entry ===", flush=True)
print(f"CDSW_WORKING_DIRECTORY={cwd}", flush=True)
print(f"CDSW_APP_PORT={port!r}", flush=True)
print(f"ROOT={ROOT}", flush=True)
print(f"dist exists={(ROOT / 'dist' / 'index.html').is_file()}", flush=True)
print(f"node_modules exists={(ROOT / 'node_modules').is_dir()}", flush=True)

if not port:
    print(
        "WARN: CDSW_APP_PORT unset (session test: export CDSW_APP_PORT=8091)",
        flush=True,
    )

try:
    import trino  # noqa: F401
except ImportError:
    req = ROOT / "requirements-cml-app.txt"
    if req.is_file():
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q", "-r", str(req)],
            check=False,
        )

if not (ROOT / "dist" / "index.html").is_file() or not (ROOT / "node_modules").is_dir():
    print("Running cdsw-build.sh …", flush=True)
    try:
        subprocess.check_call(["bash", str(ROOT / "cdsw-build.sh")], timeout=1800)
    except subprocess.CalledProcessError as exc:
        print(f"cdsw-build.sh failed: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
    except subprocess.TimeoutExpired:
        print("cdsw-build timed out — run bash cdsw-build.sh in a Session first.", file=sys.stderr)
        sys.exit(1)

runner = ROOT / "run-node-server.sh"
if not runner.is_file():
    print(f"Missing {runner}", file=sys.stderr)
    sys.exit(1)

if not port:
    print("ERROR: CDSW_APP_PORT required for Cloudera AI Application.", file=sys.stderr)
    sys.exit(1)

print("Starting Node server…", flush=True)
proc = subprocess.run(
    ["/bin/bash", str(runner)],
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
)
if proc.stdout:
    print(proc.stdout, flush=True)
if proc.returncode != 0:
    print(
        f"run-node-server.sh exited {proc.returncode} — re-run: bash cdsw-build.sh",
        file=sys.stderr,
        flush=True,
    )
sys.exit(proc.returncode)