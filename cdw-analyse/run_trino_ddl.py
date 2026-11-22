#!/usr/bin/env python3
"""Transform Impala-style create_db_tables_trino.sql and execute on CDW Trino (iceberg catalog)."""

from __future__ import annotations

import os
import re
import sys
from typing import List, Tuple

import trino
import trino.constants as trino_constants
from trino.auth import BasicAuthentication

from trino_endpoint import apply_all_trino_env

# Bumped when CDW DDL runner behavior changes (setup script checks _skip_schema_setup).
DDL_RUNNER_REV = "2025-11-22-use-skip-qualify"


def transform_sql(text: str) -> str:
    """Convert Hive/Impala DDL in create_db_tables_trino.sql to CDW Trino Iceberg syntax."""
    # Session catalog is iceberg — schema name is ``defense`` (not ``iceberg.defense``).
    text = re.sub(
        r"(?im)^create database defense;\s*",
        "CREATE SCHEMA IF NOT EXISTS defense;\n",
        text,
        count=1,
    )
    text = re.sub(
        r"(?im)^CREATE SCHEMA IF NOT EXISTS defense\s*;",
        "CREATE SCHEMA IF NOT EXISTS defense;",
        text,
    )
    text = re.sub(
        r"(?im)^CREATE SCHEMA IF NOT EXISTS iceberg\.defense\s*;",
        "CREATE SCHEMA IF NOT EXISTS defense;",
        text,
    )
    text = re.sub(r"(?im)^use defense;\s*", "USE defense;\n", text, count=1)
    text = re.sub(
        r"(?im)^USE defense\s*;",
        "USE defense;",
        text,
    )
    text = re.sub(
        r"(?im)^USE iceberg\.defense\s*;",
        "USE defense;",
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


def _qualify_prefix() -> str:
    cat = os.environ.get("TRINO_CATALOG", "iceberg")
    sch = os.environ.get("TRINO_SCHEMA", "defense")
    return f"{cat}.{sch}."


def _qualify_defense_objects(text: str) -> str:
    """Fully qualified iceberg.defense.* (matches probe_iceberg_snapshots / UI JDBC)."""
    prefix = _qualify_prefix()
    sch = os.environ.get("TRINO_SCHEMA", "defense")
    # View DDL in the SQL file often uses defense.table — expand to catalog.schema.table.
    text = re.sub(rf"(?<![\w.]){re.escape(sch)}\.", prefix, text)

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


def _skip_drop_statements(stmts: List[str]) -> List[str]:
    if os.environ.get("TRINO_SKIP_DROP", "").strip().lower() not in (
        "1",
        "true",
        "yes",
    ):
        return stmts
    kept: List[str] = []
    skipped = 0
    for s in stmts:
        st = s.strip()
        if re.match(r"(?is)^DROP\s+(TABLE|VIEW)\s+", st):
            skipped += 1
            continue
        kept.append(s)
    if skipped:
        print(f"Skipped {skipped} DROP statement(s) (TRINO_SKIP_DROP=1)")
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


def _apply_statement_path_override() -> None:
    path = os.environ.get("TRINO_STATEMENT_PATH", "").strip()
    if path:
        trino_constants.URL_STATEMENT_PATH = path


def session_schema(for_ddl: bool = True) -> str:
    """
    JDBC session schema. On CDW, DDL against iceberg.defense must use session schema
    ``defense`` (same as probe_iceberg_snapshots / trino-bridge), not ``default``.
    """
    explicit = os.environ.get("TRINO_CONNECT_SCHEMA", "").strip()
    if explicit:
        return explicit
    skip_boot = os.environ.get("TRINO_SKIP_CREATE_SCHEMA", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if skip_boot:
        return os.environ.get("TRINO_SCHEMA", "defense")
    if for_ddl and os.environ.get("TRINO_DDL_USE_TARGET_SCHEMA", "").strip().lower() not in (
        "1",
        "true",
        "yes",
    ):
        return "default"
    return os.environ.get("TRINO_SCHEMA", "defense")


def _schema_preflight_mode() -> str:
    mode = os.environ.get("TRINO_SCHEMA_PREFLIGHT", "auto").strip().lower()
    if mode in ("0", "off", "false", "no", "skip"):
        return "off"
    if mode in ("1", "force", "required", "strict"):
        return "force"
    return "auto"


def list_schemas(conn) -> Tuple[List[str] | None, str | None]:
    """Return (schema names, list_error). names is None when CDW cannot list schemas."""
    catalog = os.environ.get("TRINO_CATALOG", "iceberg")
    cur = conn.cursor()
    last_err = "unknown"
    for sql in (
        "SHOW SCHEMAS",
        f"SHOW SCHEMAS IN {catalog}",
        f"SHOW SCHEMAS FROM {catalog}",
    ):
        try:
            cur.execute(sql)
            return [str(r[0]) for r in cur.fetchall()], None
        except Exception as exc:
            last_err = str(exc)
    try:
        cur.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE catalog_name = ?",
            (catalog,),
        )
        return [str(r[0]) for r in cur.fetchall()], None
    except Exception as exc:
        last_err = str(exc)
    return None, last_err


def ensure_defense_schema(conn) -> Tuple[bool, str | None]:
    """
    Return (ready, error_message). Creates ``defense`` when allowed and missing.
    """
    mode = _schema_preflight_mode()
    if mode == "off":
        print("Preflight: skipped (TRINO_SCHEMA_PREFLIGHT=off)")
        return True, None

    sch = os.environ.get("TRINO_SCHEMA", "defense")
    names, list_err = list_schemas(conn)
    if names is None:
        if mode == "force":
            return False, (
                "Could not list schemas on Trino (CDW often returns HTTP 404 for "
                "SHOW SCHEMAS). Create **defense** in the DW UI, or set "
                "TRINO_SCHEMA_PREFLIGHT=auto to continue without listing.\n"
                f"  Detail: {list_err}"
            )
        print(
            "Preflight: could not list schemas on CDW (HTTP 404 is common) — "
            "continuing DDL."
        )
        return True, None

    if sch in names:
        print(f"Preflight: schema iceberg.{sch} is visible to Trino")
        return True, None
    if os.environ.get("TRINO_SKIP_CREATE_SCHEMA", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        return False, (
            f"Schema iceberg.{sch} is not listed by Trino (found: {', '.join(names[:12])}"
            f"{'…' if len(names) > 12 else ''}). "
            "On CDW: Data Hub → Virtual Warehouse → create schema **defense** on "
            "catalog **iceberg**, then re-run with TRINO_SKIP_CREATE_SCHEMA=1.\n"
            "Or set TRINO_SKIP_CREATE_SCHEMA=0 to run CREATE SCHEMA IF NOT EXISTS defense."
        )
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {sch}")
    except Exception as exc:
        return False, (
            f"CREATE SCHEMA IF NOT EXISTS {sch} failed: {exc}\n"
            "Create **defense** in the DW UI, then use TRINO_SKIP_CREATE_SCHEMA=1."
        )
    names2, _ = list_schemas(conn)
    if names2 is not None and sch in names2:
        print(f"Created / verified schema iceberg.{sch}")
        return True, None
    if names2 is None:
        print(f"Ran CREATE SCHEMA IF NOT EXISTS {sch}; could not re-list schemas (CDW 404)")
        return True, None
    return False, f"Schema iceberg.{sch} still missing after CREATE SCHEMA."


def connect(*, for_ddl: bool = True, schema: str | None = None):
    apply_all_trino_env()
    _apply_statement_path_override()
    host = os.environ.get(
        "TRINO_HOST",
        "cdw-triton.dw-triton-cdp-env.a465-9q4k.cloudera.site",
    )
    user = os.environ["TRINO_USER"]
    password = os.environ["TRINO_PASSWORD"]
    catalog = os.environ.get("TRINO_CATALOG", "iceberg")
    session = schema if schema is not None else session_schema(for_ddl=for_ddl)
    legacy_prep = os.environ.get("TRINO_LEGACY_PREPARED_STATEMENTS", "true").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    return trino.dbapi.connect(
        host=host,
        port=int(os.environ.get("TRINO_PORT", "443")),
        user=user,
        catalog=catalog,
        schema=session,
        http_scheme="https",
        auth=BasicAuthentication(user, password),
        legacy_prepared_statements=legacy_prep,
    )


def run_file(path: str, stop_on_error: bool = True) -> Tuple[int, List[str]]:
    raw = open(path, encoding="utf-8").read()
    transformed = transform_sql(raw)
    stmts = _skip_drop_statements(_skip_schema_setup(split_statements(transformed)))
    conn = connect(for_ddl=True)
    ready, pre_err = ensure_defense_schema(conn)
    if not ready:
        conn.close()
        return 0, [pre_err or "iceberg.defense not ready"]
    sch = os.environ.get("TRINO_SCHEMA", "defense")
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
            if (
                "404" in str(exc)
                and re.match(r"(?is)^DROP\s+TABLE\s+IF\s+EXISTS\b", stmt)
                and os.environ.get("TRINO_FORGIVE_DROP_404", "1").strip().lower()
                in ("1", "true", "yes")
            ):
                ok += 1
                continue
            if "404" in str(exc) and sch in stmt:
                msg += (
                    f"\n  Hint: HTTP 404 on iceberg.{sch} usually means the schema "
                    "is missing in the metastore (not just the table). Create "
                    "**defense** in the CDW DW UI, or TRINO_SKIP_CREATE_SCHEMA=0 "
                    "to attempt CREATE SCHEMA IF NOT EXISTS defense."
                )
            elif re.match(r"(?is)^CREATE\s+SCHEMA", stmt.strip()) and "404" in str(exc):
                msg += (
                    "\n  Hint: Create schema **defense** in the Data Warehouse UI."
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
