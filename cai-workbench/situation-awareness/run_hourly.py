#!/usr/bin/env python3
"""CDSW/CML scheduled entry: hourly Baltic situation awareness report."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _install_job_root(root: Path) -> Path:
    root = root.resolve()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    os.chdir(root)
    return root


def _resolve_job_root() -> Path:
    markers = ("run_hourly.py", "entry.py")
    mod = globals().get("__file__")
    if mod:
        root = Path(mod).resolve().parent
        if any((root / m).is_file() for m in markers):
            return root
    work = os.environ.get("CDSW_WORKING_DIRECTORY", "").strip()
    if work in ("", "/"):
        work = ""
    candidates: list[Path] = []
    if work:
        base = Path(work)
        candidates.extend(
            [
                base / "cai-workbench" / "situation-awareness",
                base / "situation-awareness",
                base,
            ]
        )
    home = Path.home()
    candidates.extend(
        [
            home / "cai-workbench" / "situation-awareness",
            home,
            Path("/home/cdsw") / "cai-workbench" / "situation-awareness",
        ]
    )
    candidates.extend([Path.cwd(), Path.cwd() / "situation-awareness"])
    seen: set[Path] = set()
    for c in candidates:
        try:
            cr = c.resolve()
        except OSError:
            continue
        if cr in seen:
            continue
        seen.add(cr)
        if any((cr / m).is_file() for m in markers):
            return cr
    raise RuntimeError(
        "Lagebild job root not found. CML Scheduled Job command:\n"
        "  bash /home/cdsw/cai-workbench/situation-awareness/run-from-session.sh\n"
        "Do not paste run_hourly.py into a notebook cell."
    )


_ROOT = _install_job_root(_resolve_job_root())

from lagebild import execute_sql_insert, run_pipeline
from src.config import Settings


def main() -> int:
    parser = argparse.ArgumentParser(description="Hourly Lagebild LLM report job")
    parser.add_argument(
        "--use-sample",
        action="store_true",
        help="Force offline sample CSV (sets LAGEBILD_USE_SAMPLE)",
    )
    parser.add_argument(
        "--skip-llm",
        action="store_true",
        help="Build PDF from dataset summary only (no model call)",
    )
    args = parser.parse_args()

    if args.use_sample:
        import os

        os.environ["LAGEBILD_USE_SAMPLE"] = "1"

    settings = Settings()
    settings.artifacts_dir = (_ROOT / settings.artifacts_dir).resolve()

    try:
        result = run_pipeline(settings, skip_llm=args.skip_llm)
    except Exception as exc:
        print(f"Lagebild job failed: {exc}", file=sys.stderr)
        return 1

    print(f"Rows used: {result.row_count}")
    print(f"Data source: {result.data_source}")
    print(f"Dataset summary: {result.dataset_summary}")
    if result.pdf_path:
        print(f"PDF: {result.pdf_path}")
    if result.comparison_text:
        print("\n--- Comparison ---\n")
        print(result.comparison_text[:4000])

    if result.sql_insert and settings.insert_sql_enabled:
        try:
            execute_sql_insert(settings, result.sql_insert)
            print("SQL insert into defense.Situation_Awareness_Summary complete.")
        except Exception as exc:
            print(f"SQL insert failed: {exc}", file=sys.stderr)
            return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
