#!/usr/bin/env python3
"""Execute a Trino SQL file under cdw-analyse via run_trino_ddl."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def repo_root() -> Path:
    for base in (
        Path("/home/cdsw"),
        Path.cwd(),
        Path(__file__).resolve().parents[2],
    ):
        if (base / "cdw-analyse" / "run_trino_ddl.py").is_file():
            return base.resolve()
    raise RuntimeError("Repository root not found (expected cdw-analyse/run_trino_ddl.py)")


def main() -> int:
    root = repo_root()
    cdw = root / "cdw-analyse"
    if str(cdw) not in sys.path:
        sys.path.insert(0, str(cdw))

    job = os.environ.get("TRINO_JOB", "outliers").strip().lower()
    if job in ("outliers", "outlier", "ais_outliers"):
        sql_rel = "outlier_detection_trino.sql"
        os.environ.setdefault("TRINO_CONTINUE_ON_ERROR", "1")
        stop_on_error = False
    elif job in ("lagebild_views", "lagebild", "views"):
        from run_lagebild_views import main as views_main

        os.chdir(cdw)
        return views_main()
    else:
        sql_rel = os.environ.get("TRINO_SQL_FILE", "").strip()
        if not sql_rel:
            print(
                f"Unknown TRINO_JOB={job!r}; use outliers|lagebild_views or set TRINO_SQL_FILE",
                file=sys.stderr,
            )
            return 2
        stop_on_error = os.environ.get("TRINO_STOP_ON_ERROR", "0") == "1"

    sql_path = (root / sql_rel) if not Path(sql_rel).is_absolute() else Path(sql_rel)
    if not sql_path.is_file():
        sql_path = cdw / sql_rel
    if not sql_path.is_file():
        print(f"SQL file not found: {sql_path}", file=sys.stderr)
        return 2

    from run_trino_ddl import run_file

    os.chdir(cdw)
    ok, errors = run_file(str(sql_path), stop_on_error=stop_on_error)
    print(f"OK statements: {ok}")
    for e in errors:
        print("\nERROR:\n", e)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())