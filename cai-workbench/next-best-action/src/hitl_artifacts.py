"""Decision record and HITL-facing JSON artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .alert_types import NbaAlert
from .decision_record import build_decision_record
from .grounding import build_grounding
from .maritime_incident_state import MaritimeIncidentState
from .red_blue_gameplay import run_red_blue_episode
from .rules_engine import evaluate_playbook, playbook_recommended_coa
from .scenario_matrix import run_scenario_matrix
from .scoring import CoaScore, EnvironmentContext
from .snapshot_bundle import SnapshotBundle


def finalize_hitl_artifacts(
    *,
    artifact_dir: Path,
    alert: NbaAlert,
    bundle: SnapshotBundle,
    env: EnvironmentContext,
    ranked: list[CoaScore],
    best_coa_id: str | None,
    sensor_gates: dict[str, Any] | None,
    simulations: list[Any] | None,
    agent_state: MaritimeIncidentState | None,
    ticks: int = 6,
    materialized: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    legal = evaluate_playbook(alert, ranked)
    playbook_best = playbook_recommended_coa(legal, ranked, best_coa_id)
    grounding = build_grounding(
        alert,
        legal,
        ranked,
        playbook_best=playbook_best,
        best_coa_id=best_coa_id,
        sensor_gates=sensor_gates,
    )
    scenario_runs = run_scenario_matrix(
        alert, env, coa_id=playbook_best or best_coa_id, ticks=ticks
    )
    coa_play = playbook_best or best_coa_id or "A"
    gameplay = run_red_blue_episode(
        alert,
        env,
        coa_id=coa_play,
        red_doctrine="evasive",
        ticks=ticks,
    )
    what_if_replays = [
        run_red_blue_episode(
            alert, env, coa_id=coa_play, red_doctrine=d, ticks=ticks
        ).to_dict()
        for d in ("evasive", "aggressive", "cooperative")
    ]

    ranked_dicts = [s.to_dict() for s in ranked]
    sim_dicts = [s.to_dict() for s in simulations] if simulations else None

    record = build_decision_record(
        alert=alert,
        bundle=bundle,
        ranked_coas=ranked_dicts,
        best_coa_id=best_coa_id,
        sensor_gates=sensor_gates,
        simulations=sim_dicts,
        grounding=grounding,
        agent_state=agent_state,
        scenario_runs=scenario_runs,
        gameplay=gameplay,
        materialized=materialized,
        what_if_replays=what_if_replays,
    )

    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "grounding.json").write_text(
        json.dumps(grounding.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8"
    )
    (artifact_dir / "scenario_matrix.json").write_text(
        json.dumps([r.to_dict() for r in scenario_runs], indent=2), encoding="utf-8"
    )
    (artifact_dir / "red_blue_replay.json").write_text(
        json.dumps(gameplay.to_dict(), indent=2), encoding="utf-8"
    )
    (artifact_dir / "what_if_red_doctrines.json").write_text(
        json.dumps(what_if_replays, indent=2), encoding="utf-8"
    )
    decision_path = artifact_dir / "decision_record.json"
    decision_path.write_text(
        json.dumps(record.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8"
    )

    if agent_state is not None:
        agent_state.grounding = grounding.to_dict()
        agent_state.scenario_runs = [r.to_dict() for r in scenario_runs]
        agent_state.agentic_gameplay = gameplay.to_dict()
        agent_state.decision_record = record.to_dict()
        agent_state.playbook_best_coa_id = playbook_best
        from .state_persist import persist_state

        persist_state(agent_state, artifact_dir)

    return record.to_dict()