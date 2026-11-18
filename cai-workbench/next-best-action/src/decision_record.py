"""Operator decision card (§9) — single JSON artifact for HITL."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .alert_types import NbaAlert
from .grounding import IncidentGrounding
from .maritime_incident_state import MaritimeIncidentState
from .red_blue_gameplay import RedBlueReplay
from .scenario_matrix import ScenarioRun
from .snapshot_bundle import SnapshotBundle


@dataclass
class DecisionRecord:
    flow_uuid: str
    generated_at: str
    sections: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "flow_uuid": self.flow_uuid,
            "generated_at": self.generated_at,
            "sections": self.sections,
        }


def build_decision_record(
    *,
    alert: NbaAlert,
    bundle: SnapshotBundle | None,
    ranked_coas: list[dict[str, Any]],
    best_coa_id: str | None,
    sensor_gates: dict[str, Any] | None,
    simulations: list[dict[str, Any]] | None,
    grounding: IncidentGrounding,
    agent_state: MaritimeIncidentState | None,
    scenario_runs: list[ScenarioRun] | None,
    gameplay: RedBlueReplay | None,
    materialized: list[dict[str, Any]] | None = None,
    what_if_replays: list[dict[str, Any]] | None = None,
) -> DecisionRecord:
    now = datetime.now(timezone.utc).isoformat()
    legal = {}
    narrative = None
    playbook_best = None
    llm_used = False
    if agent_state:
        legal = agent_state.legal_assessment or {}
        narrative = agent_state.agent_narrative
        playbook_best = agent_state.playbook_best_coa_id
        llm_used = bool(agent_state.llm_used)

    sections: dict[str, Any] = {
        "1_what_happened": {
            "theatre": alert.theatre,
            "alert_type": alert.alert_type,
            "priority": alert.priority,
            "position": {"lat": alert.latitude, "lon": alert.longitude},
            "triggers": (alert.payload or {}).get("triggers"),
            "payload_excerpt": alert.payload,
        },
        "2_data_pin": {
            "flow_uuid": alert.flow_uuid,
            "snapshots": bundle.to_dict()["snapshots"] if bundle else [],
            "materialized_tables": materialized or [],
            "lagebild_note": "Lagebild live-only; facts pinned via snapshot bundle.",
        },
        "3_ranked_coas": {
            "best_coa_id": best_coa_id,
            "playbook_best_coa_id": playbook_best,
            "coa_scores": ranked_coas,
        },
        "4_rules_and_ontology": {
            "grounding": grounding.to_dict(),
            "legal_assessment": legal,
        },
        "5_sim_outcomes": {
            "simulations": simulations or [],
            "sensor_gates": sensor_gates,
        },
        "6_narrative": {
            "text": narrative,
            "llm_used": llm_used,
            "citation_rule_ids": grounding.rule_citations,
        },
        "7_hitl_approval": {
            "status_hint": "Operator approves via UI; writes approved_coa_id to Trino.",
        },
        "8_scenario_matrix": {
            "runs": [r.to_dict() for r in (scenario_runs or [])],
        },
        "9_agentic_gameplay": {
            "replay": gameplay.to_dict() if gameplay else None,
            "what_if_red_doctrines": what_if_replays or [],
        },
    }

    return DecisionRecord(
        flow_uuid=alert.flow_uuid,
        generated_at=now,
        sections=sections,
    )