#!/usr/bin/env python3
"""CDSW/CML scheduled entry: hourly Baltic situation awareness report."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from cml_root import install_job_root, resolve_job_root

# CML: run as script (python3 run_hourly.py) — not as notebook cell (__file__ undefined).
_ROOT = install_job_root(resolve_job_root("run_hourly.py", "entry.py"))

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
