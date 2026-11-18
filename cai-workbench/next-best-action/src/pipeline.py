"""NBA pipeline: Phase 0 base + Phase 1 simulation adapter."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .agent_flow import run_agent_flow
from .agent_orchestrator import run_orchestrator_closure
from .governance_export import export_governance_bundle
from .hitl_artifacts import finalize_hitl_artifacts
from .alert_rules import next_alert
from .alert_types import NbaAlert
from .config import Settings
from .maritime_incident_state import MaritimeIncidentState
from .env_gates import evaluate_sensor_gates
from .materialize import materialize_bundle
from .persistence import (
    insert_coa_scores,
    insert_incident,
    insert_materialized_tables,
    insert_snapshots,
    update_decision_artifacts,
)
from .scoring import EnvironmentContext, rank_coas
from .sim_adapter import apply_simulation_to_scores, simulate_passing_coas
from .snapshot_bundle import SnapshotBundle, SnapshotCapture, capture_bundle


@dataclass
class PipelineResult:
    alert: NbaAlert
    bundle: SnapshotBundle
    ranked_coas: list
    best_coa_id: str | None
    artifact_dir: Path
    simulations: list | None = None
    sensor_gates: dict[str, Any] | None = None
    agent_state: MaritimeIncidentState | None = None


def _write_artifacts(
    out_dir: Path,
    *,
    phase: int,
    alert: NbaAlert,
    bundle: SnapshotBundle,
    env: EnvironmentContext,
    ranked: list,
    best: str | None,
    simulations: list | None,
    sensor_gates: dict | None,
    materialized: list | None = None,
    mcp_audit: dict[str, Any] | None = None,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    audit: dict[str, Any] = {
        "phase": phase,
        "alert": alert.to_dict(),
        "snapshot_bundle": bundle.to_dict(),
        "environment": {
            "cloud_cover_pct": env.cloud_cover_pct,
            "night": env.night,
            "seastate": env.seastate,
            "turbidity_high": env.turbidity_high,
            "water_depth_m": env.water_depth_m,
        },
        "sensor_gates": sensor_gates,
        "coa_scores": [s.to_dict() for s in ranked],
        "best_coa_id": best,
    }
    if simulations:
        audit["simulations"] = [s.to_dict() for s in simulations]
    if materialized:
        audit["materialized_tables"] = materialized
    if mcp_audit:
        audit["snapshot_capture_source"] = mcp_audit.get("capture_source")
        audit["iceberg_mcp"] = {
            "capture_source": mcp_audit.get("capture_source"),
            "captured_at": mcp_audit.get("captured_at"),
        }
        (out_dir / "mcp_audit.json").write_text(
            json.dumps(mcp_audit, indent=2, default=str), encoding="utf-8"
        )
    (out_dir / "audit.json").write_text(
        json.dumps(audit, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    (out_dir / "coa_matrix.json").write_text(
        json.dumps([s.to_dict() for s in ranked], indent=2), encoding="utf-8"
    )
    if simulations:
        (out_dir / "simulation_trace.json").write_text(
            json.dumps([s.to_dict() for s in simulations], indent=2),
            encoding="utf-8",
        )
    return audit


def run_phase0(settings: Settings | None = None) -> PipelineResult:
    settings = settings or Settings()
    settings.phase = 0
    return _run_pipeline(settings)


def run_phase1(settings: Settings | None = None) -> PipelineResult:
    settings = settings or Settings()
    settings.phase = 1
    return _run_pipeline(settings)


def run_phase3(settings: Settings | None = None) -> PipelineResult:
    settings = settings or Settings()
    settings.phase = 3
    return _run_pipeline(settings)


def run_phase7(settings: Settings | None = None) -> PipelineResult:
    settings = settings or Settings()
    settings.phase = 7
    return _run_pipeline(settings)


def run_pipeline(settings: Settings | None = None) -> PipelineResult:
    settings = settings or Settings()
    if settings.phase >= 7:
        return run_phase7(settings)
    if settings.phase >= 3:
        return run_phase3(settings)
    if settings.phase >= 1:
        return run_phase1(settings)
    return run_phase0(settings)


def _run_pipeline(settings: Settings) -> PipelineResult:
    settings.artifacts_dir = Path(settings.artifacts_dir)
    alert = next_alert(settings)
    snap_capture: SnapshotCapture = capture_bundle(alert.flow_uuid, settings)
    bundle = snap_capture.bundle
    mcp_audit = snap_capture.mcp_audit
    materialized_dicts: list[dict[str, Any]] = []
    mat_results = []
    if settings.materialize_facts:
        mat_results = materialize_bundle(bundle, settings)
        materialized_dicts = [m.to_dict() for m in mat_results]
    env = EnvironmentContext.from_alert_payload(alert.payload)
    gates = evaluate_sensor_gates(env)
    ranked = rank_coas(env)
    simulations = None

    if settings.phase >= 1:
        simulations = simulate_passing_coas(
            alert, env, ticks=settings.sim_ticks
        )
        ranked = apply_simulation_to_scores(ranked, simulations)

    best = next((s.coa.coa_id for s in ranked if s.gates_passed), None)
    out_dir = settings.artifacts_dir / alert.flow_uuid

    audit = _write_artifacts(
        out_dir,
        phase=settings.phase,
        alert=alert,
        bundle=bundle,
        env=env,
        ranked=ranked,
        best=best,
        simulations=simulations,
        sensor_gates=gates.to_dict(),
        materialized=materialized_dicts or None,
        mcp_audit=mcp_audit,
    )
    if materialized_dicts:
        (out_dir / "materialized_tables.json").write_text(
            json.dumps(materialized_dicts, indent=2), encoding="utf-8"
        )

    agent_state = None
    decision: dict[str, Any] | None = None
    if settings.phase >= 3:
        agent_state = run_agent_flow(
            settings=settings,
            alert=alert,
            bundle=bundle,
            ranked=ranked,
            best_coa_id=best,
            artifact_dir=out_dir,
            sensor_gates=gates.to_dict(),
        )
        audit["playbook_best_coa_id"] = agent_state.playbook_best_coa_id
        audit["agent_narrative_preview"] = (agent_state.agent_narrative or "")[:400]
        (out_dir / "audit.json").write_text(
            json.dumps(audit, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    if settings.phase >= 1:
        decision = finalize_hitl_artifacts(
            artifact_dir=out_dir,
            alert=alert,
            bundle=bundle,
            env=env,
            ranked=ranked,
            best_coa_id=best,
            sensor_gates=gates.to_dict(),
            simulations=simulations,
            agent_state=agent_state,
            ticks=settings.sim_ticks,
            materialized=materialized_dicts or None,
            settings=settings,
        )
        audit["decision_record"] = {
            "generated_at": decision["generated_at"],
            "sections_present": list(decision.get("sections", {}).keys()),
        }
        if decision.get("shacl"):
            audit["shacl"] = {
                "conforms": decision["shacl"].get("conforms"),
                "engine": decision["shacl"].get("engine"),
            }
        (out_dir / "audit.json").write_text(
            json.dumps(audit, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    if settings.phase >= 7 and decision and agent_state is not None:
        sim_dicts = [s.to_dict() for s in simulations] if simulations else None
        agent_state = run_orchestrator_closure(
            agent_state,
            out_dir,
            simulations=sim_dicts,
            decision_record=decision,
        )
        gov = export_governance_bundle(
            out_dir,
            flow_uuid=alert.flow_uuid,
            decision_record=decision,
        )
        audit["governance_bundle"] = {
            "exported_at": gov["exported_at"],
            "artifact_count": len(gov["artifact_manifest"]),
        }
        (out_dir / "audit.json").write_text(
            json.dumps(audit, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    if settings.persist_trino and best is not None:
        insert_incident(alert, "SCORED", best, settings)
        insert_snapshots(bundle, settings)
        if mat_results:
            insert_materialized_tables(alert.flow_uuid, mat_results, settings)
        insert_coa_scores(alert.flow_uuid, ranked, settings)
        if settings.phase >= 1:
            grounding_dict = None
            grounding_path = out_dir / "grounding.json"
            if grounding_path.is_file():
                grounding_dict = json.loads(
                    grounding_path.read_text(encoding="utf-8")
                )
            dec_path = out_dir / "decision_record.json"
            if dec_path.is_file():
                decision_dict = json.loads(dec_path.read_text(encoding="utf-8"))
                shacl_meta = decision.get("shacl") if decision else None
                update_decision_artifacts(
                    alert.flow_uuid,
                    decision_record=decision_dict,
                    grounding=grounding_dict,
                    settings=settings,
                    shacl_report=shacl_meta,
                )

    return PipelineResult(
        alert=alert,
        bundle=bundle,
        ranked_coas=ranked,
        best_coa_id=best,
        artifact_dir=out_dir,
        simulations=simulations,
        sensor_gates=gates.to_dict(),
        agent_state=agent_state,
    )