#!/usr/bin/env python3
"""
Cloudera AI Application entry (required Python host script).

Listens via Node/Express on CDSW_APP_PORT (or CDSW_READONLY_PORT).
See: https://docs.cloudera.com/machine-learning/cloud/applications/topics/ml-applications-c.html
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)

# Cloudera AI injects the public application port here.
port = (
    os.environ.get("CDSW_APP_PORT")
    or os.environ.get("CDSW_READONLY_PORT")
    or os.environ.get("API_PORT")
    or "8090"
)
os.environ["CDSW_APP_PORT"] = str(port)
os.environ["API_PORT"] = str(port)
os.environ.setdefault("NODE_ENV", "production")

dist_index = ROOT / "dist" / "index.html"
if not dist_index.is_file():
    print("dist/ missing — running cdsw-build.sh (Node + vite build)…", flush=True)
    subprocess.check_call(["bash", str(ROOT / "cdsw-build.sh")])


def resolve_node() -> str:
    node = "node"
    which = subprocess.run(["which", "node"], capture_output=True, text=True)
    if which.returncode == 0 and which.stdout.strip():
        return which.stdout.strip()
    nvm = Path.home() / ".nvm" / "nvm.sh"
    if nvm.is_file():
        # Find node via login shell that sources nvm
        out = subprocess.run(
            ["bash", "-lc", "source ~/.nvm/nvm.sh && command -v node"],
            capture_output=True,
            text=True,
        )
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    for pattern in (Path.home() / "node-v20.20.2-linux-x64",):
        candidate = pattern / "bin" / "node"
        if candidate.is_file():
            return str(candidate)
    for glob in sorted(Path.home().glob("node-v20.*-linux-x64")):
        candidate = glob / "bin" / "node"
        if candidate.is_file():
            return str(candidate)
    return node


node_bin = resolve_node()
server = ROOT / "server" / "index.js"
if not server.is_file():
    print(f"Missing {server}", file=sys.stderr)
    sys.exit(1)

print(f"Triton NATO UI → port {port} (node={node_bin})", flush=True)
os.execv(node_bin, [node_bin, str(server)])