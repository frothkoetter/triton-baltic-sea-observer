#!/usr/bin/env python3
"""Trino read/execute for NBA HITL (Iceberg defense). JSON on stdout."""
from __future__ import annotations

import json
import os
import re
import sys

import trino
from trino.auth import BasicAuthentication


def normalize_trino_coordinator_host(host: str) -> str:
    """DW overview hostname `cdw-triton.*` is not the Trino statement API (`triton-trino.*`)."""
    if re.match(r"^cdw-triton\.", host, re.I):
        return re.sub(r"^cdw-triton\.", "triton-trino.", host, count=1, flags=re.I)
    return host


def apply_jdbc_url_env() -> None:
    url = os.environ.get("TRINO_JDBC_URL", "").strip()
    if not url:
        return
    m = re.match(
        r"^jdbc:trino://([^:/?#]+)(?::(\d+))?(?:/([^/?#]+))?(?:/([^/?#]+))?",
        url,
        re.IGNORECASE,
    )
    if not m:
        raise ValueError(f"Invalid TRINO_JDBC_URL: {url}")
    host, port, catalog, schema = m.groups()
    host = normalize_trino_coordinator_host(host)
    os.environ["TRINO_HOST"] = host
    if port:
        os.environ.setdefault("TRINO_PORT", port)
    if catalog:
        os.environ.setdefault("TRINO_CATALOG", catalog)
    if schema:
        os.environ.setdefault("TRINO_SCHEMA", schema)


def connect():
    apply_jdbc_url_env()
    user = os.environ.get("TRINO_USER", "")
    password = os.environ.get("TRINO_PASSWORD", "")
    if not user or not password:
        raise RuntimeError("Set TRINO_USER and TRINO_PASSWORD (or TRINO_JDBC_URL)")
    # Password only via BasicAuthentication — dbapi.connect() has no password= kwarg.
    raw_host = os.environ.get(
        "TRINO_HOST",
        "triton-trino.dw-triton-cdp-env.a465-9q4k.cloudera.site",
    )
    return trino.dbapi.connect(
        host=normalize_trino_coordinator_host(raw_host),
        port=int(os.environ.get("TRINO_PORT", "443")),
        user=user,
        catalog=os.environ.get("TRINO_CATALOG", "iceberg"),
        schema=os.environ.get("TRINO_SCHEMA", "defense"),
        http_scheme="https",
        auth=BasicAuthentication(user, password),
    )


def main() -> None:
    if len(sys.argv) < 2:
        print(json.dumps({"ok": False, "error": "Usage: trino-bridge.py <sql> [query|execute]"}))
        sys.exit(1)
    sql = sys.argv[1]
    mode = sys.argv[2] if len(sys.argv) > 2 else "query"
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql)
        if mode == "execute":
            conn.close()
            print(json.dumps({"ok": True, "source": "trino", "rows_affected": cur.rowcount}))
            return
        columns = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(500)
        conn.close()
        data = []
        for row in rows:
            item = {}
            for col, val in zip(columns, row):
                if hasattr(val, "isoformat"):
                    item[col] = val.isoformat()
                else:
                    item[col] = val
            data.append(item)
        print(
            json.dumps(
                {"ok": True, "columns": columns, "rows": data, "source": "trino"},
                default=str,
            )
        )
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc), "source": "trino"}))
        sys.exit(2)


if __name__ == "__main__":
    main()