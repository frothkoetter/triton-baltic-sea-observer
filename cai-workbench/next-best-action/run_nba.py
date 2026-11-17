#!/usr/bin/env python3
"""CML entry: NBA Phase 0 (snapshot bundle + COA scoring, no LLM)."""

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
    markers = ("run_nba.py", "entry.py")
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
                base / "cai-workbench" / "next-best-action",
                base / "next-best-action",
                base,
            ]
        )
    home = Path.home()
    candidates.extend(
        [
            home / "cai-workbench" / "next-best-action",
            home,
            Path("/home/cdsw") / "cai-workbench" / "next-best-action",
        ]
    )
    candidates.extend([Path.cwd(), Path.cwd() / "next-best-action"])
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
        "NBA job root not found. CML Scheduled Job command must be:\n"
        "  bash /home/cdsw/cai-workbench/next-best-action/run-from-session.sh ...\n"
        "Do not paste run_nba.py into a notebook cell (Cell In[...])."
    )


_ROOT = _install_job_root(_resolve_job_root())


def _filter_kernel_argv(argv: list[str] | None = None) -> list[str]:
    """Strip Jupyter/CML ipykernel `-f kernel-….json` (not real job flags)."""
    raw = list(argv if argv is not None else sys.argv[1:])
    cleaned: list[str] = []
    skip_next = False
    for arg in raw:
        if skip_next:
            skip_next = False
            continue
        if arg in ("-f", "--f"):
            skip_next = True
            continue
        if "kernel-" in arg and arg.endswith(".json"):
            continue
        cleaned.append(arg)
    return cleaned


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
        choices=(0, 1, 3),
        default=None,
        help="Pipeline phase: 0=COA only, 1=+sim, 3=+playbook/LLM (default NBA_PHASE)",
    )
    parser.add_argument(
        "--skip-llm",
        action="store_true",
        help="Phase 3: template narrative only (no model call)",
    )
    args, _unknown = parser.parse_known_args(_filter_kernel_argv())

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
    if args.skip_llm:
        os.environ["NBA_SKIP_LLM"] = "1"

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
    if result.agent_state:
        print(f"playbook_best_coa_id={result.agent_state.playbook_best_coa_id}")
        print(f"incident_state={result.artifact_dir}/incident_state.json")
        if result.agent_state.llm_used:
            print("agent_llm=1")
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


def _exit_job(rc: int) -> None:
    in_kernel = (
        "ipykernel" in sys.modules
        or "IPython" in sys.modules
        or (bool(sys.argv) and "ipykernel" in sys.argv[0])
    )
    if in_kernel and rc == 0:
        return
    raise SystemExit(rc)


if __name__ == "__main__":
    _exit_job(main())