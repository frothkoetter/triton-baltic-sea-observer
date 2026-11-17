#!/usr/bin/env python3
"""CML entry: NBA Phase 0 (snapshot bundle + COA scoring, no LLM)."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.config import Settings
from src.pipeline import run_phase0


def main() -> int:
    parser = argparse.ArgumentParser(description="NBA Phase 0 job")
    parser.add_argument(
        "--demo-greifswald",
        action="store_true",
        help="Force Greifswald/Adlergrund demo alert",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Skip Trino snapshot capture",
    )
    parser.add_argument(
        "--persist-trino",
        action="store_true",
        help="Write nba_* tables on CDW Trino",
    )
    args = parser.parse_args()

    if args.demo_greifswald:
        os.environ["NBA_DEMO_GREIFSWALD"] = "1"
    if args.offline:
        os.environ["NBA_OFFLINE"] = "1"
    if args.persist_trino:
        os.environ["NBA_PERSIST_TRINO"] = "1"

    settings = Settings()
    settings.artifacts_dir = (_ROOT / settings.artifacts_dir).resolve()

    try:
        result = run_phase0(settings)
    except Exception as exc:
        print(f"NBA job failed: {exc}", file=sys.stderr)
        return 1

    print(f"flow_uuid={result.alert.flow_uuid}")
    print(f"theatre={result.alert.theatre}")
    print(f"best_coa_id={result.best_coa_id}")
    print(f"artifacts={result.artifact_dir}")
    for score in result.ranked_coas:
        flag = "PASS" if score.gates_passed else "FAIL"
        print(
            f"  COA {score.coa.coa_id} [{flag}] total={score.score_total:.3f} "
            f"{score.coa.title[:60]}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())