#!/usr/bin/env python3
"""Quick CDW Trino checks before bulk DDL (.env.trino.local)."""

from __future__ import annotations

import os
import sys

from run_trino_ddl import connect, session_schema
from trino_endpoint import apply_all_trino_env, probe_trino_http

_AUTH_HINT = """
HTTP 401 / "Access Denied: Invalid credentials" — Trino host is correct; fix login:

  1. Use the same user/password you use for **Cloudera Data Warehouse / CDP console**
     (workload LDAP password — not an Impala keytab, not CML API key).
  2. In `.env.trino.local` use plain assignment (no quotes in the file unless intentional):
     TRINO_USER=frothkoetter
     TRINO_PASSWORD=your-cdp-password
     (Avoid trailing spaces; re-type password if copied from a password manager.)
  3. If your tenant logs in with email, try TRINO_USER=you@company.com .
  4. Confirm the user has **Data Warehouse User** / query access on this VW.
  5. Verify with Trino CLI (same Copy Trino URL):
     trino --server 'https://triton-trino....:443' --user YOU --password \\
       --catalog iceberg --schema defense
     then: SELECT version();
  6. After a bad password was pasted in chat, reset it in CDP User Management.
"""


def _try(cur, label: str, sql: str) -> tuple[bool, bool]:
    """Returns (ok, saw_401)."""
    try:
        cur.execute(sql)
        if cur.description:
            row = cur.fetchone()
            print(f"  OK  {label}: {row}")
        else:
            print(f"  OK  {label}")
        return True, False
    except Exception as exc:
        msg = str(exc)
        saw_401 = "401" in msg or "Invalid credentials" in msg or "Access Denied" in msg
        print(f"  FAIL {label}: {exc}")
        return False, saw_401


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
    user = os.environ.get("TRINO_USER", "")
    print(f"TRINO_USER={user!r} (password set: {'yes' if os.environ.get('TRINO_PASSWORD') else 'no'})")

    print("\n==> HTTP probe (Trino POST /v1/statement)")
    ok_http, url, logs = probe_trino_http()
    for line in logs:
        print(f"  {line}")

    auth_rejected = any("HTTP 401" in line for line in logs) or any(
        "401 =" in line for line in logs
    )

    if auth_rejected:
        print(_AUTH_HINT)
        return 3

    if not ok_http:
        print(
            "\nAll HTTP probes failed (often HTTP 404). This is **not** a DDL/schema bug.\n"
            "Fix connectivity first:\n"
            "  1. CDW → Virtual Warehouse → state **Running**.\n"
            "  2. ⋮ → **Copy Trino URL** → TRINO_SERVER='https://triton-trino....:443'\n"
            "     (not the old `cdw-triton` DW hostname).\n"
            "  4. See cdw-analyse/README_CDW_TRINO_LOCAL.md\n"
        )
        return 2
    print(f"  OK  Trino REST endpoint: {url}")

    print("\n==> Python trino client")
    conn = connect(for_ddl=True)
    cur = conn.cursor()
    db_ok = True
    any_401 = False
    ok, a = _try(cur, "version", "SELECT version()")
    db_ok &= ok
    any_401 |= a
    ok, a = _try(cur, "session", "SELECT current_catalog, current_schema")
    db_ok &= ok
    any_401 |= a
    ok, a = _try(cur, "show_tables", f"SHOW TABLES IN {sch}")
    db_ok &= ok
    any_401 |= a
    conn.close()
    if any_401:
        print(_AUTH_HINT)
        return 3
    if not db_ok:
        print(
            "\nHTTP probe succeeded but dbapi failed — try TRINO_LEGACY_PREPARED_STATEMENTS=true "
            "or align TRINO_SERVER with the working HTTP URL."
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())