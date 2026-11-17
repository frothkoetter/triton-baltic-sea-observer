#!/usr/bin/env python3
"""
Cloudera AI Application entry — must listen on CDSW_APP_PORT.

Starts Node via run-node-server.sh (see Cloudera Applications docs).
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
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

req = ROOT / "requirements-cml-app.txt"
if req.is_file():
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q", "-r", str(req)],
        check=False,
    )

if not (ROOT / "dist" / "index.html").is_file() or not (ROOT / "node_modules").is_dir():
    print("Running cdsw-build.sh (first-time / missing artifacts)…", flush=True)
    try:
        subprocess.check_call(["bash", str(ROOT / "cdsw-build.sh")], timeout=1800)
    except subprocess.CalledProcessError as exc:
        print(f"cdsw-build.sh failed: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
    except subprocess.TimeoutExpired:
        print("cdsw-build.sh timed out (30m). Build once in a Session, then restart app.", file=sys.stderr)
        sys.exit(1)

runner = ROOT / "run-node-server.sh"
if not runner.is_file():
    print(f"Missing {runner}", file=sys.stderr)
    sys.exit(1)

print("Starting Node server…", flush=True)
try:
    os.execv("/bin/bash", ["/bin/bash", str(runner)])
except OSError as exc:
    print(f"exec failed: {exc}", file=sys.stderr)
    sys.exit(1)