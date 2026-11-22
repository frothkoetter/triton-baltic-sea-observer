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
    text = re.sub(
        r"(?im)^CREATE SCHEMA IF NOT EXISTS defense\s*;",
        "CREATE SCHEMA IF NOT EXISTS iceberg.defense;",
        text,
    )
    text = re.sub(r"(?im)^use defense;\s*", "USE iceberg.defense;\n", text, count=1)
    text = re.sub(
        r"(?im)^USE defense\s*;",
        "USE iceberg.defense;",
        text,
    )
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

    if _should_qualify_schema():
        text = _qualify_defense_objects(text)

    return text


def _should_qualify_schema() -> bool:
    if os.environ.get("TRINO_QUALIFY_SCHEMA", "").strip().lower() in ("1", "true", "yes"):
        return True
    return os.environ.get("TRINO_SKIP_CREATE_SCHEMA", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _qualify_defense_objects(text: str) -> str:
    """Session on iceberg.default — objects live in iceberg.defense.* (no USE)."""
    cat = os.environ.get("TRINO_CATALOG", "iceberg")
    sch = os.environ.get("TRINO_SCHEMA", "defense")
    prefix = f"{cat}.{sch}."

    def _qualify_table_refs(body: str) -> str:
        # View bodies: FROM ships / JOIN ships without catalog (DDL file convention).
        return re.sub(
            r"(?i)\b(FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)\b",
            lambda m: f"{m.group(1)} {prefix}{m.group(2)}"
            if "." not in m.group(2) and m.group(2).lower() not in ("select", "lateral")
            else m.group(0),
            body,
        )

    def _qualify_stmt_head(m: re.Match[str]) -> str:
        kw = m.group(1)
        name = m.group(2)
        if "." in name:
            return m.group(0)
        return f"{kw} {prefix}{name}"

    text = re.sub(
        r"(?im)^(CREATE TABLE|DROP TABLE IF EXISTS|INSERT INTO)\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        _qualify_stmt_head,
        text,
    )
    text = re.sub(
        r"(?im)^(CREATE OR REPLACE VIEW)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s+",
        lambda m: f"{m.group(1)} {prefix}{m.group(2)} AS "
        if "." not in m.group(2)
        else m.group(0),
        text,
    )
    # Qualify view SELECT bodies (statement-local; multi-line views in one stmt).
    parts = re.split(r"(?im)^(CREATE OR REPLACE VIEW)", text)
    if len(parts) > 1:
        out = [parts[0]]
        for i in range(1, len(parts), 2):
            header = "CREATE OR REPLACE VIEW" + parts[i]
            rest = parts[i + 1] if i + 1 < len(parts) else ""
            out.append(_qualify_table_refs(header + rest))
        text = "".join(out)
    return text


def _skip_schema_setup(stmts: List[str]) -> List[str]:
    if os.environ.get("TRINO_SKIP_CREATE_SCHEMA", "").strip().lower() not in (
        "1",
        "true",
        "yes",
    ):
        return stmts
    kept: List[str] = []
    skipped = 0
    for s in stmts:
        st = s.strip()
        if re.match(r"(?is)^CREATE\s+SCHEMA\b", st):
            skipped += 1
            continue
        if re.match(r"(?is)^USE\s+", st):
            skipped += 1
            continue
        kept.append(s)
    if skipped:
        print(
            f"Skipped {skipped} CREATE SCHEMA / USE statement(s) (TRINO_SKIP_CREATE_SCHEMA)"
        )
    return kept


def split_statements(text: str) -> List[str]:
    lines: List[str] = []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    cleaned = "\n".join(lines)
    parts = re.split(r";\s*\n", cleaned)
    return [p.strip() for p in parts if p.strip()]


def apply_jdbc_url_env() -> None:
    """Populate TRINO_HOST/PORT/CATALOG/SCHEMA from TRINO_JDBC_URL when set."""
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
    os.environ.setdefault("TRINO_HOST", host)
    if port:
        os.environ.setdefault("TRINO_PORT", port)
    if catalog:
        os.environ.setdefault("TRINO_CATALOG", catalog)
    if schema:
        os.environ.setdefault("TRINO_SCHEMA", schema)


def session_schema(for_ddl: bool = True) -> str:
    """
    JDBC session schema. For greenfield DDL, connect to an existing schema
    (iceberg.default) — not TRINO_SCHEMA=defense before CREATE SCHEMA runs.
    """
    explicit = os.environ.get("TRINO_CONNECT_SCHEMA", "").strip()
    if explicit:
        return explicit
    if for_ddl and os.environ.get("TRINO_DDL_USE_TARGET_SCHEMA", "").strip().lower() not in (
        "1",
        "true",
        "yes",
    ):
        return "default"
    return os.environ.get("TRINO_SCHEMA", "defense")


def connect(*, for_ddl: bool = True, schema: str | None = None):
    apply_jdbc_url_env()
    host = os.environ.get(
        "TRINO_HOST",
        "cdw-triton.dw-triton-cdp-env.a465-9q4k.cloudera.site",
    )
    user = os.environ["TRINO_USER"]
    password = os.environ["TRINO_PASSWORD"]
    catalog = os.environ.get("TRINO_CATALOG", "iceberg")
    session = schema if schema is not None else session_schema(for_ddl=for_ddl)
    return trino.dbapi.connect(
        host=host,
        port=int(os.environ.get("TRINO_PORT", "443")),
        user=user,
        catalog=catalog,
        schema=session,
        http_scheme="https",
        auth=BasicAuthentication(user, password),
    )


def run_file(path: str, stop_on_error: bool = True) -> Tuple[int, List[str]]:
    raw = open(path, encoding="utf-8").read()
    transformed = transform_sql(raw)
    stmts = _skip_schema_setup(split_statements(transformed))
    conn = connect(for_ddl=True)
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
            if re.match(r"(?is)^CREATE\s+SCHEMA", stmt.strip()) and "404" in str(exc):
                msg += (
                    "\n  Hint: On CDW, schema defense may already exist or must be "
                    "created in the Data Warehouse / metastore UI — not via Trino. "
                    "Re-run with TRINO_SKIP_CREATE_SCHEMA=1 if iceberg.defense exists."
                )
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
    continue_on_error = len(sys.argv) > 2 or os.environ.get("TRINO_CONTINUE_ON_ERROR") == "1"
    ok, errors = run_file(path, stop_on_error=not continue_on_error)
    print(f"OK statements: {ok}")
    for e in errors:
        print("\nERROR:\n", e)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
