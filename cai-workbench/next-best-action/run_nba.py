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

from src.alert_rules import NoAlertFound
from src.config import Settings
from src.pipeline import run_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(description="NBA Phase 0 job")
    parser.add_argument(
        "--demo-greifswald",
        action="store_true",
        help="Force Greifswald/Adlergrund demo alert",
    )
    parser.add_argument(
        "--no-demo",
        action="store_true",
        help="Event-driven mode: detect alerts from Trino/SITREP (no Greifswald)",
    )
    parser.add_argument(
        "--exit-if-quiet",
        action="store_true",
        help="Exit 0 when no alert is found (CML schedule idle)",
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
    parser.add_argument(
        "--phase",
        type=int,
        choices=(0, 1),
        default=None,
        help="Pipeline phase (default: NBA_PHASE env or 1)",
    )
    args = parser.parse_args()

    if args.demo_greifswald:
        os.environ["NBA_DEMO_GREIFSWALD"] = "1"
    if args.no_demo:
        os.environ["NBA_DEMO_GREIFSWALD"] = "0"
    if args.exit_if_quiet:
        os.environ["NBA_EXIT_IF_QUIET"] = "1"
    if args.offline:
        os.environ["NBA_OFFLINE"] = "1"
    if args.persist_trino:
        os.environ["NBA_PERSIST_TRINO"] = "1"

    settings = Settings()
    settings.artifacts_dir = (_ROOT / settings.artifacts_dir).resolve()

    if args.phase is not None:
        os.environ["NBA_PHASE"] = str(args.phase)
        settings = Settings()

    try:
        result = run_pipeline(settings)
    except NoAlertFound as exc:
        if settings.exit_if_quiet:
            print(f"NBA idle: {exc}")
            return 0
        print(f"NBA job failed: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"NBA job failed: {exc}", file=sys.stderr)
        return 1

    print(f"flow_uuid={result.alert.flow_uuid}")
    print(f"theatre={result.alert.theatre}")
    print(f"best_coa_id={result.best_coa_id}")
    print(f"artifacts={result.artifact_dir}")
    if result.sensor_gates:
        print(f"sensor_gates={result.sensor_gates}")
    if result.simulations:
        for sim in result.simulations:
            print(
                f"  sim COA {sim.coa_id}: discovery={sim.final_discovery:.3f} "
                f"ticks={sim.ticks}"
            )
    for score in result.ranked_coas:
        flag = "PASS" if score.gates_passed else "FAIL"
        print(
            f"  COA {score.coa.coa_id} [{flag}] total={score.score_total:.3f} "
            f"{score.coa.title[:60]}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())