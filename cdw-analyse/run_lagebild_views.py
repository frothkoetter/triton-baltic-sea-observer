#!/usr/bin/env python3
"""Re-apply defense.lagebild and defense.lagebild_all_events from create_db_tables_trino.sql."""

from __future__ import annotations

import os
import re
import sys

from run_trino_ddl import connect, transform_sql

_VIEW_NAMES = ("lagebild", "lagebild_all_events")

_ORDER_BY_TAIL = re.compile(
    r"\nORDER BY harbour_name,\s*\"timestamp\"\s+DESC\s*;?\s*",
    re.IGNORECASE,
)


def _sanitize_view_body(body: str) -> str:
    """Trino rejects ORDER BY inside CREATE VIEW; drop statement-splitting semicolons."""
    body = _ORDER_BY_TAIL.sub("\n", body)
    lines = body.rstrip().splitlines()
    while lines:
        tail = lines[-1].strip()
        if not tail or tail == ";" or tail.startswith("--"):
            lines.pop()
            continue
        break
    if lines:
        lines[-1] = re.sub(r";\s*$", "", lines[-1]).rstrip()
    return "\n".join(lines).strip()


def _repo_sql_path() -> str:
    return os.path.join(os.path.dirname(__file__), "create_db_tables_trino.sql")


def extract_view_statements(text: str) -> list[str]:
    """Return CREATE OR REPLACE VIEW statements for lagebild views (with trailing semicolon)."""
    transformed = transform_sql(text)
    out: list[str] = []
    for name in _VIEW_NAMES:
        marker = f"CREATE OR REPLACE VIEW {name} AS"
        start = transformed.find(marker)
        if start < 0:
            raise ValueError(f"Missing {marker} in create_db_tables_trino.sql")
        rest = transformed[start + len(marker) :]
        next_create = re.search(r"\nCREATE OR REPLACE VIEW \w+ AS", rest)
        if name == _VIEW_NAMES[-1]:
            stop = re.search(r"\nDROP TABLE\b", rest)
            body_end = stop.start() if stop else len(rest)
        else:
            boundary = _ORDER_BY_TAIL.search(rest)
            if not boundary and next_create:
                boundary = next_create
            body_end = boundary.start() if boundary else len(rest)
        body = _sanitize_view_body(rest[:body_end].strip())
        # Trino dbapi: do not terminate the CREATE VIEW query with an inner ';'
        stmt = f"{marker}\n{body}".strip()
        out.append(stmt)
    return out


def run_views(path: str | None = None) -> tuple[int, list[str]]:
    sql_path = path or _repo_sql_path()
    text = open(sql_path, encoding="utf-8").read()
    stmts = extract_view_statements(text)
    conn = connect()
    cur = conn.cursor()
    errors: list[str] = []
    ok = 0
    for i, stmt in enumerate(stmts, 1):
        preview = stmt.split("\n", 1)[0][:120]
        try:
            cur.execute(stmt)
            ok += 1
        except Exception as exc:
            errors.append(f"#{i} {preview}\n  {exc}")
            break
    conn.close()
    return ok, errors


def main() -> int:
    if not os.environ.get("TRINO_USER") or not os.environ.get("TRINO_PASSWORD"):
        print("Set TRINO_USER and TRINO_PASSWORD", file=sys.stderr)
        return 2
    path = sys.argv[1] if len(sys.argv) > 1 else None
    ok, errors = run_views(path)
    print(f"OK statements: {ok}/{len(_VIEW_NAMES)}")
    for e in errors:
        print("\nERROR:\n", e)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())