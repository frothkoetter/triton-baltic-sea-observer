#!/usr/bin/env python3
"""Apply sql/06_tier_a_observation_areas.sql to live Trino (Iceberg defense)."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SQL_FILE = ROOT / "sql" / "06_tier_a_observation_areas.sql"

import importlib.util

BRIDGE = Path(__file__).resolve().parents[3] / "triton-ui" / "server" / "trino-bridge.py"
_spec = importlib.util.spec_from_file_location("trino_bridge", BRIDGE)
_mod = importlib.util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(_mod)
connect = _mod.connect


def split_statements(sql: str) -> list[str]:
    parts = re.split(r";\s*\n", sql.strip())
    return [p.strip().rstrip(";") for p in parts if p.strip() and not p.strip().startswith("--")]


def main() -> int:
    if not SQL_FILE.is_file():
        print(f"Missing {SQL_FILE}; run build_emodnet_tier_a_restricted.py first")
        return 1
    sql = SQL_FILE.read_text(encoding="utf-8")
    stmts = split_statements(sql)
    conn = connect()
    cur = conn.cursor()
    for stmt in stmts:
        if stmt.upper().startswith("USE "):
            continue
        print(f"Executing: {stmt[:72]}...")
        cur.execute(stmt)
    conn.close()
    print(f"Applied {len(stmts)} statements from {SQL_FILE.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())