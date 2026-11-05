#!/usr/bin/env python3
"""Transform Impala-style create_db_tables_trino.sql and execute on CDW Trino (iceberg catalog)."""

from __future__ import annotations

import os
import re
import sys
from typing import List, Tuple

import trino
from trino.auth import BasicAuthentication


def transform_sql(text: str) -> str:
    """Convert Hive/Impala DDL in create_db_tables_trino.sql to CDW Trino Iceberg syntax."""
    text = re.sub(
        r"(?im)^create database defense;\s*",
        "CREATE SCHEMA IF NOT EXISTS iceberg.defense;\n",
        text,
        count=1,
    )
    text = re.sub(r"(?im)^use defense;\s*", "USE iceberg.defense;\n", text, count=1)
    text = re.sub(r"`([a-zA-Z_][a-zA-Z0-9_]*)`", r"\1", text)
    text = re.sub(r"(?im)^CREATE EXTERNAL TABLE", "CREATE TABLE", text)
    text = re.sub(r"(?im)^INSERT INTO TABLE\b", "INSERT INTO", text)

    text = re.sub(
        r"\)\s*\nPARTITIONED BY SPEC \(TRUNCATE\(10, Event_Timestamp\)\)\s*\nSTORED by ICEBERG\s*;",
        ") WITH (partitioning = ARRAY['truncate(event_timestamp, 10)']);",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\)\s*\nPARTITIONED BY SPEC \(TRUNCATE\(10, ts\)\)\s*\nSTORED by ICEBERG\s*;",
        ") WITH (partitioning = ARRAY['truncate(ts, 10)']);",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\)\s*\nPARTITIONED BY SPEC \(day\(ingested_at\)\)\s*\nSTORED BY ICEBERG\s*;",
        ") WITH (partitioning = ARRAY['day(ingested_at)']);",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\)\s*\nCOMMENT 'Table for maritime surveillance reports'\s*\nSTORED by ICEBERG\s*;",
        ");",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\)\s*\nSTORED BY ICEBERG\s*;",
        ");",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\)\s*\nSTORED by ICEBERG\s*;",
        ");",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\)\s*\nSTORED BY Iceberg\s*;",
        ");",
        text,
        flags=re.IGNORECASE,
    )

    # Hive types -> Trino types (DDL only; safe on this script file).
    text = re.sub(r"\bSTRING\b", "varchar", text)
    text = re.sub(r"\bINT\b", "integer", text)
    text = re.sub(r"\bBIGINT\b", "bigint", text)
    text = re.sub(r"\bDOUBLE\b", "double", text)

    text = re.sub(r"(?im)^create view\b", "CREATE OR REPLACE VIEW", text)
    text = re.sub(r"(?im)^drop table if exists\b", "DROP TABLE IF EXISTS", text)
    text = re.sub(r"(?im)^drop table\b(?! if)", "DROP TABLE IF EXISTS", text)

    return text


def split_statements(text: str) -> List[str]:
    lines: List[str] = []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    cleaned = "\n".join(lines)
    parts = re.split(r";\s*\n", cleaned)
    return [p.strip() for p in parts if p.strip()]


def connect():
    host = os.environ.get(
        "TRINO_HOST",
        "cdw-aw-se-trino.dw-se-sandbox-aws.a465-9q4k.cloudera.site",
    )
    user = os.environ["TRINO_USER"]
    password = os.environ["TRINO_PASSWORD"]
    return trino.dbapi.connect(
        host=host,
        port=int(os.environ.get("TRINO_PORT", "443")),
        user=user,
        catalog=os.environ.get("TRINO_CATALOG", "iceberg"),
        schema=os.environ.get("TRINO_SCHEMA", "defense"),
        http_scheme="https",
        auth=BasicAuthentication(user, password),
    )


def run_file(path: str, stop_on_error: bool = True) -> Tuple[int, List[str]]:
    raw = open(path, encoding="utf-8").read()
    transformed = transform_sql(raw)
    stmts = split_statements(transformed)
    conn = connect()
    cur = conn.cursor()
    errors: List[str] = []
    ok = 0
    for i, stmt in enumerate(stmts, 1):
        stmt = stmt.strip().rstrip(";")
        preview = stmt.split("\n", 1)[0][:120]
        try:
            cur.execute(stmt)
            ok += 1
        except Exception as exc:
            msg = f"#{i} {preview}\n  {exc}"
            errors.append(msg)
            if stop_on_error:
                break
    conn.close()
    return ok, errors


def main() -> int:
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(__file__), "create_db_tables_trino.sql"
    )
    if not os.environ.get("TRINO_USER") or not os.environ.get("TRINO_PASSWORD"):
        print("Set TRINO_USER and TRINO_PASSWORD", file=sys.stderr)
        return 2
    ok, errors = run_file(path, stop_on_error=len(sys.argv) <= 2)
    print(f"OK statements: {ok}")
    for e in errors:
        print("\nERROR:\n", e)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
