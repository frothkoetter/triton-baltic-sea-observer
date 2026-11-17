#!/usr/bin/env python3
"""Probe which Iceberg snapshot / branch features work on CDW Trino (iceberg.defense).

Usage:
  set -a && source .env.trino.local && set +a
  python3 probe_iceberg_snapshots.py [--table lagebild]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from run_trino_ddl import connect


@dataclass
class ProbeResult:
    name: str
    sql: str
    ok: bool
    detail: str = ""
    rows: list[Any] = field(default_factory=list)


def run_query(cur, sql: str, limit_preview: int = 3) -> tuple[bool, str, list]:
    try:
        cur.execute(sql)
        if cur.description is None:
            return True, "OK (no result set)", []
        cols = [d[0] for d in cur.description]
        rows = cur.fetchmany(limit_preview)
        preview = [dict(zip(cols, row)) for row in rows]
        return True, f"OK ({len(cols)} cols)", preview
    except Exception as exc:
        return False, str(exc), []


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--table",
        default="lagebild",
        help="Iceberg table name in iceberg.defense (default: lagebild)",
    )
    parser.add_argument("--write-probes", action="store_true", help="Run DDL branch/rollback probes on a temp table")
    args = parser.parse_args()

    catalog = "iceberg"
    schema = "defense"
    table = args.table
    fq = f"{catalog}.{schema}.{table}"
    probes: list[tuple[str, str]] = []

    probes.append(("version", "SELECT version()"))

    # Metadata table name variants (Trino vs Hive-style on CDW)
    meta_variants = [
        f'SELECT * FROM {catalog}.{schema}."{table}$snapshots" ORDER BY committed_at DESC LIMIT 5',
        f"SELECT * FROM {fq}.snapshots ORDER BY committed_at DESC LIMIT 5",
        f"SELECT * FROM {fq}.HISTORY ORDER BY made_current_at DESC LIMIT 5",
        f'SELECT * FROM {catalog}.{schema}."{table}$refs" LIMIT 10',
        f"SELECT * FROM {fq}.REFS LIMIT 10",
    ]
    for i, sql in enumerate(meta_variants):
        probes.append((f"meta_{i}", sql))

    conn = connect()
    cur = conn.cursor()

    results: list[ProbeResult] = []
    snapshot_id: int | None = None
    snapshot_ts: str | None = None

    for name, sql in probes:
        ok, detail, rows = run_query(cur, sql)
        results.append(ProbeResult(name=name, sql=sql, ok=ok, detail=detail, rows=rows))
        if ok and name.startswith("meta_") and rows and snapshot_id is None:
            row0 = rows[0]
            sid = row0.get("snapshot_id") or row0.get("snapshotid")
            if sid is not None:
                snapshot_id = int(sid)
            for key in ("committed_at", "made_current_at", "timestamp_ms"):
                if key in row0 and row0[key]:
                    snapshot_ts = str(row0[key])
                    break

    if snapshot_id is not None:
        tt_probes = [
            (
                "time_travel_version",
                f"SELECT COUNT(*) AS c FROM {fq} FOR VERSION AS OF {snapshot_id}",
            ),
            (
                "time_travel_system_version",
                f"SELECT COUNT(*) AS c FROM {fq} FOR SYSTEM_VERSION AS OF {snapshot_id}",
            ),
        ]
        if snapshot_ts:
            tt_probes.append(
                (
                    "time_travel_timestamp",
                    f"SELECT COUNT(*) AS c FROM {fq} FOR TIMESTAMP AS OF TIMESTAMP '{snapshot_ts}'",
                )
            )
            tt_probes.append(
                (
                    "time_travel_system_time",
                    f"SELECT COUNT(*) AS c FROM {fq} FOR SYSTEM_TIME AS OF TIMESTAMP '{snapshot_ts}'",
                )
            )
        for name, sql in tt_probes:
            ok, detail, rows = run_query(cur, sql)
            results.append(ProbeResult(name=name, sql=sql, ok=ok, detail=detail, rows=rows))

    branch_probes = [
        (
            "create_branch_sql",
            f"CREATE BRANCH nba_probe_{int(time.time())} IN TABLE {fq}",
        ),
        (
            "alter_create_branch_hive",
            f"ALTER TABLE {fq} CREATE BRANCH nba_probe_hive",
        ),
        (
            "show_branches",
            f"SHOW BRANCHES IN TABLE {fq}",
        ),
    ]
    if args.write_probes:
        for name, sql in branch_probes:
            ok, detail, rows = run_query(cur, sql)
            results.append(ProbeResult(name=name, sql=sql, ok=ok, detail=detail, rows=rows))
        if snapshot_id is not None:
            rollback_sql = f"ALTER TABLE {fq} EXECUTE rollback_to_snapshot({snapshot_id})"
            ok, detail, rows = run_query(cur, rollback_sql)
            results.append(
                ProbeResult(name="rollback_to_snapshot", sql=rollback_sql, ok=ok, detail=detail, rows=rows)
            )
    else:
        for name, sql in branch_probes:
            results.append(
                ProbeResult(
                    name=name,
                    sql=sql,
                    ok=False,
                    detail="skipped (pass --write-probes to test DDL)",
                )
            )

    conn.close()

    summary = {
        "table": fq,
        "snapshot_id_used": snapshot_id,
        "snapshot_ts_used": snapshot_ts,
        "probes": [
            {
                "name": r.name,
                "ok": r.ok,
                "detail": r.detail,
                "sql": r.sql,
                "rows_preview": r.rows,
            }
            for r in results
        ],
    }
    print(json.dumps(summary, indent=2, default=str))

    failed = [r for r in results if not r.ok and "skipped" not in r.detail]
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())