#!/usr/bin/env python3
"""Quick CDW Trino checks before bulk DDL (.env.trino.local)."""

from __future__ import annotations

import os
import sys

from run_trino_ddl import connect, session_schema


def _try(cur, label: str, sql: str) -> bool:
    try:
        cur.execute(sql)
        if cur.description:
            row = cur.fetchone()
            print(f"  OK  {label}: {row}")
        else:
            print(f"  OK  {label}")
        return True
    except Exception as exc:
        print(f"  FAIL {label}: {exc}")
        return False


def main() -> int:
    if not os.environ.get("TRINO_USER") or not os.environ.get("TRINO_PASSWORD"):
        print("Doctor skipped: set TRINO_USER / TRINO_PASSWORD", file=sys.stderr)
        return 0
    cat = os.environ.get("TRINO_CATALOG", "iceberg")
    sch = os.environ.get("TRINO_SCHEMA", "defense")
    sess = session_schema(for_ddl=True)
    print(f"Session catalog={cat} schema={sess} (target {cat}.{sch})")
    conn = connect(for_ddl=True)
    cur = conn.cursor()
    ok = True
    ok &= _try(cur, "version", "SELECT version()")
    ok &= _try(
        cur,
        "session",
        f"SELECT current_catalog, current_schema",
    )
    ok &= _try(cur, "show_tables", f"SHOW TABLES IN {sch}")
    conn.close()
    if not ok:
        print(
            "\nIf all checks fail with HTTP 404: confirm Virtual Warehouse is Running, "
            f"catalog name is '{cat}', schema '{sch}' exists on that catalog, and "
            "TRINO_CONNECT_SCHEMA=defense (not default) in .env.trino.local."
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())