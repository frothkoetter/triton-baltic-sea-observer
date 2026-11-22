"""Trino JDBC using the same env vars as cdw-analyse/run_trino_ddl.py."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import trino
from trino.auth import BasicAuthentication

_CDW = Path(__file__).resolve().parents[3] / "cdw-analyse"
if _CDW.is_dir() and str(_CDW) not in sys.path:
    sys.path.insert(0, str(_CDW))

from trino_endpoint import apply_all_trino_env  # noqa: E402
from run_trino_ddl import connect  # noqa: E402


def trino_configured() -> bool:
    apply_all_trino_env()
    return bool(os.environ.get("TRINO_USER") and os.environ.get("TRINO_PASSWORD"))


def get_connection():
    if not trino_configured():
        raise RuntimeError("Set TRINO_USER and TRINO_PASSWORD (e.g. .env.trino.local)")
    return connect(for_ddl=False)


def fetch_all(sql: str) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        if cur.description is None:
            return []
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def execute_sql(sql: str) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
    finally:
        conn.close()