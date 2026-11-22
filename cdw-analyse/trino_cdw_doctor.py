#!/usr/bin/env python3
"""Quick CDW Trino checks before bulk DDL (.env.trino.local)."""

from __future__ import annotations

import os
import sys

from run_trino_ddl import connect, session_schema
from trino_endpoint import apply_all_trino_env, probe_trino_http


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
    apply_all_trino_env()
    host = os.environ.get("TRINO_HOST", "")
    port = os.environ.get("TRINO_PORT", "443")
    cat = os.environ.get("TRINO_CATALOG", "iceberg")
    sch = os.environ.get("TRINO_SCHEMA", "defense")
    sess = session_schema(for_ddl=True)

    print(f"Coordinator: https://{host}:{port}")
    print(f"Session catalog={cat} schema={sess} (target {cat}.{sch})")

    print("\n==> HTTP probe (Trino POST /v1/statement)")
    ok_http, url, logs = probe_trino_http()
    for line in logs:
        print(f"  {line}")

    if not ok_http:
        print(
            "\nAll HTTP probes returned 404 or failed. This is **not** a DDL/schema bug.\n"
            "Fix connectivity first:\n"
            "  1. CDW → Virtual Warehouse **cdw-triton** → state must be **Running** (resume if stopped).\n"
            "  2. ⋮ menu → **Copy Trino URL** and set in .env.trino.local:\n"
            "       TRINO_SERVER='<paste https://...>'\n"
            "     (Use the Trino URL, not the DW JDBC string from docs/examples.)\n"
            "  3. Match TRINO_USER / TRINO_PASSWORD to a DW user with query rights.\n"
            "  4. Optional: trino CLI with the same Copy Trino URL — `SELECT version()` must work.\n"
        )
        return 2
    print(f"  OK  Trino REST endpoint: {url}")

    print("\n==> Python trino client")
    conn = connect(for_ddl=True)
    cur = conn.cursor()
    db_ok = True
    db_ok &= _try(cur, "version", "SELECT version()")
    db_ok &= _try(cur, "session", "SELECT current_catalog, current_schema")
    db_ok &= _try(cur, "show_tables", f"SHOW TABLES IN {sch}")
    conn.close()
    if not db_ok:
        print(
            "\nHTTP probe succeeded but dbapi failed — try TRINO_LEGACY_PREPARED_STATEMENTS=true "
            "or align TRINO_SERVER with the working HTTP URL."
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())