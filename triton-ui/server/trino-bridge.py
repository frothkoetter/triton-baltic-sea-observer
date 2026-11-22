#!/usr/bin/env python3
"""Trino read/execute for NBA HITL (Iceberg defense). JSON on stdout."""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

import trino
from trino.auth import BasicAuthentication

_DEFAULT_HOST = "triton-trino.dw-triton-cdp-env.a465-9q4k.cloudera.site"


def _cdw_analyse_dir() -> Path | None:
    here = Path(__file__).resolve()
    for base in (here.parent, *here.parents):
        cand = base / "cdw-analyse"
        if (cand / "trino_endpoint.py").is_file():
            return cand
    repo = here.parents[1] if len(here.parents) > 1 else None
    if repo and (repo / "cdw-analyse" / "trino_endpoint.py").is_file():
        return repo / "cdw-analyse"
    return None


def apply_trino_env() -> None:
    cdw = _cdw_analyse_dir()
    if cdw is not None:
        root = str(cdw)
        if root not in sys.path:
            sys.path.insert(0, root)
        from trino_endpoint import apply_all_trino_env, apply_statement_path_to_client

        apply_all_trino_env()
        apply_statement_path_to_client()
        return

    url = os.environ.get("TRINO_JDBC_URL", "").strip()
    if url:
        m = re.match(
            r"^jdbc:trino://([^:/?#]+)(?::(\d+))?(?:/([^/?#]+))?(?:/([^/?#]+))?",
            url,
            re.IGNORECASE,
        )
        if not m:
            raise ValueError(f"Invalid TRINO_JDBC_URL: {url}")
        host, port, catalog, schema = m.groups()
        if re.match(r"^cdw-triton\.", host, re.I):
            host = re.sub(r"^cdw-triton\.", "triton-trino.", host, count=1, flags=re.I)
        os.environ["TRINO_HOST"] = host
        if port:
            os.environ.setdefault("TRINO_PORT", port)
        if catalog:
            os.environ.setdefault("TRINO_CATALOG", catalog)
        if schema:
            os.environ.setdefault("TRINO_SCHEMA", schema)


def resolved_host() -> str:
    apply_trino_env()
    host = os.environ.get("TRINO_HOST", _DEFAULT_HOST).strip()
    if re.match(r"^cdw-triton\.", host, re.I):
        host = re.sub(r"^cdw-triton\.", "triton-trino.", host, count=1, flags=re.I)
        os.environ["TRINO_HOST"] = host
    return host


def connect():
    apply_trino_env()
    host = resolved_host()
    user = os.environ.get("TRINO_USER", "")
    password = os.environ.get("TRINO_PASSWORD", "")
    if not user or not password:
        raise RuntimeError("Set TRINO_USER and TRINO_PASSWORD (or TRINO_JDBC_URL)")
    return trino.dbapi.connect(
        host=host,
        port=int(os.environ.get("TRINO_PORT", "443")),
        user=user,
        catalog=os.environ.get("TRINO_CATALOG", "iceberg"),
        schema=os.environ.get("TRINO_SCHEMA", "defense"),
        http_scheme="https",
        auth=BasicAuthentication(user, password),
    )


def _hint_404(exc: BaseException) -> str:
    host = os.environ.get("TRINO_HOST", "(unset)")
    jdbc = os.environ.get("TRINO_JDBC_URL", "")
    msg = (
        f"Coordinator TRINO_HOST={host!r}. "
        "Use CDW Virtual Warehouse -> Copy Trino URL (triton-trino.*), "
        "not cdw-triton DW overview. "
    )
    if "cdw-triton" in jdbc.lower():
        msg += "TRINO_JDBC_URL still contains cdw-triton — fix CML Project/Application env and restart."
    return msg


def env_check_json() -> dict:
    apply_trino_env()
    host = resolved_host()
    return {
        "ok": True,
        "trinoHost": host,
        "trinoUserSet": bool(os.environ.get("TRINO_USER")),
        "trinoPasswordSet": bool(os.environ.get("TRINO_PASSWORD")),
        "trinoJdbcHost": (
            re.match(r"^jdbc:trino://([^:/?#]+)", os.environ.get("TRINO_JDBC_URL", ""), re.I)
            or [None, None]
        )[1],
        "trinoServer": os.environ.get("TRINO_SERVER", ""),
        "statementPath": os.environ.get("TRINO_STATEMENT_PATH", ""),
    }


def main() -> None:
    if len(sys.argv) >= 2 and sys.argv[1] in ("--env-check", "-E"):
        print(json.dumps(env_check_json()))
        return
    if len(sys.argv) < 2:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "Usage: trino-bridge.py <sql> [query|execute] | --env-check",
                }
            )
        )
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
                {
                    "ok": True,
                    "columns": columns,
                    "rows": data,
                    "source": "trino",
                    "trinoHost": os.environ.get("TRINO_HOST"),
                },
                default=str,
            )
        )
    except Exception as exc:
        err = str(exc)
        payload = {"ok": False, "error": err, "source": "trino", "trinoHost": os.environ.get("TRINO_HOST")}
        if "404" in err:
            payload["hint"] = _hint_404(exc)
        print(json.dumps(payload))
        sys.exit(2)


if __name__ == "__main__":
    main()