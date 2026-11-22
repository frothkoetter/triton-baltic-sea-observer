#!/usr/bin/env python3
"""Cloudera AI Application entry — listen on CDSW_APP_PORT via Node/Express."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def resolve_root() -> Path:
    """Project root (triton-ui). Works as script; not in Jupyter cells."""
    if globals().get("__file__"):
        return Path(__file__).resolve().parent
    work = os.environ.get("CDSW_WORKING_DIRECTORY", "").strip()
    if work:
        for candidate in (Path(work) / "triton-ui", Path(work)):
            if (candidate / "server" / "index.js").is_file():
                return candidate
    here = Path.cwd()
    if (here / "server" / "index.js").is_file():
        return here
    nested = here / "triton-ui"
    if (nested / "server" / "index.js").is_file():
        return nested
    raise RuntimeError(
        "Cannot locate triton-ui. Run: python3 triton-ui/entry.py "
        "(not from a notebook cell). CDSW_WORKING_DIRECTORY="
        f"{work or '(unset)'}"
    )


ROOT = resolve_root()
os.chdir(ROOT)
os.environ["NVM_DIR"] = str(ROOT / ".nvm")

app_port = os.environ.get("CDSW_APP_PORT", "").strip()
ro_port = os.environ.get("CDSW_READONLY_PORT", "").strip()
api_port = os.environ.get("API_PORT", "").strip()
# Platform-injected port for the Application script (never override in Application env).
port = app_port or ro_port
cwd = os.environ.get("CDSW_WORKING_DIRECTORY", os.getcwd())

print("=== triton-ui entry ===", flush=True)
print(f"CDSW_WORKING_DIRECTORY={cwd}", flush=True)
print(f"CDSW_APP_PORT={app_port!r}", flush=True)
print(f"CDSW_READONLY_PORT={ro_port!r}", flush=True)
if api_port:
    print(
        f"WARN: API_PORT={api_port!r} in env — remove from Application/Project "
        "(Node uses CDSW_APP_PORT only)",
        flush=True,
    )
if app_port and ro_port and app_port != ro_port:
    print(
        "NOTE: CDSW_APP_PORT and CDSW_READONLY_PORT differ — "
        "subdomain proxy usually matches ONE of them (see Engine container log "
        "'port scope' for tritonnato).",
        flush=True,
    )
print(f"ROOT={ROOT}", flush=True)
print(f"dist exists={(ROOT / 'dist' / 'index.html').is_file()}", flush=True)
print(f"node_modules exists={(ROOT / 'node_modules').is_dir()}", flush=True)

if not port:
    print(
        "WARN: CDSW_APP_PORT unset (session test: export CDSW_APP_PORT=8091)",
        flush=True,
    )

_BRIDGE_PYTHON_MODULES = ("trino", "boto3", "laspy", "numpy", "h5py")


def _import_missing(module: str) -> bool:
    try:
        __import__(module)
        return False
    except ImportError:
        return True


def ensure_cml_python_deps() -> None:
    """Install LAS/S3/Trino bridge deps (laspy is often missing when trino is preinstalled)."""
    req = ROOT / "requirements-cml-app.txt"
    if not req.is_file():
        return
    missing = [m for m in _BRIDGE_PYTHON_MODULES if _import_missing(m)]
    if not missing:
        return
    print(
        f"Installing Python bridge deps ({', '.join(missing)}) from {req.name} …",
        flush=True,
    )
    rc = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q", "-r", str(req)],
        check=False,
    ).returncode
    if rc != 0:
        print(f"WARN: pip install exited {rc}", flush=True)
    still = [m for m in missing if _import_missing(m)]
    if still:
        print(
            f"WARN: still missing after pip: {', '.join(still)} "
            f"(Session: {sys.executable} -m pip install -r {req})",
            flush=True,
        )


ensure_cml_python_deps()


def _ui_bundle_stale() -> bool:
    """True when committed React sources are newer than the baked Vite dist/."""
    dist_index = ROOT / "dist" / "index.html"
    if not dist_index.is_file():
        return True
    force = os.environ.get("TRITON_FORCE_UI_REBUILD", "").strip().lower()
    if force in ("1", "true", "yes"):
        print("TRITON_FORCE_UI_REBUILD set — rebuilding UI bundle", flush=True)
        return True
    dist_mtime = dist_index.stat().st_mtime
    markers = [ROOT / "package.json", ROOT / "vite.config.ts", ROOT / "index.html", ROOT / "build-stamp.json"]
    src_dir = ROOT / "src"
    if src_dir.is_dir():
        for path in src_dir.rglob("*"):
            if path.is_file():
                markers.append(path)
    newest = max((p.stat().st_mtime for p in markers if p.is_file()), default=dist_mtime)
    if newest > dist_mtime + 1.0:
        print(
            "UI bundle is stale (src newer than dist/) — will run cdsw-build.sh",
            flush=True,
        )
        return True
    return False


needs_ui_build = (
    not (ROOT / "node_modules").is_dir()
    or not (ROOT / "dist" / "index.html").is_file()
    or _ui_bundle_stale()
)

if needs_ui_build:
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
rc = subprocess.call(["/bin/bash", str(runner)])
if rc != 0:
    print(
        f"run-node-server.sh exited {rc} — in Session: bash cdsw-build.sh",
        file=sys.stderr,
        flush=True,
    )
sys.exit(rc)