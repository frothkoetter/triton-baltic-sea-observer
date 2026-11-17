"""Phase 3 flow: playbook → optional LLM narrative → persisted state."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .alert_types import NbaAlert
from .config import Settings
from . import llm_client
from .maritime_incident_state import MaritimeIncidentState
from .rules_engine import (
    LegalAssessment,
    evaluate_playbook,
    playbook_recommended_coa,
)
from .scoring import CoaScore
from .snapshot_bundle import SnapshotBundle
from .state_persist import persist_state


def _template_narrative(
    alert: NbaAlert,
    best: str | None,
    legal: LegalAssessment,
) -> str:
    lines = [
        f"Lage {alert.theatre}: {alert.alert_type} ({alert.priority}).",
        f"Empfohlene COA: {best or 'keine'}.",
    ]
    if legal.messages:
        lines.append("Playbook: " + " | ".join(legal.messages[:3]))
    return "\n\n".join(lines)


def run_agent_flow(
    *,
    settings: Settings,
    alert: NbaAlert,
    bundle: SnapshotBundle,
    ranked: list[CoaScore],
    best_coa_id: str | None,
    artifact_dir: Path,
    sensor_gates: dict[str, Any] | None,
) -> MaritimeIncidentState:
    legal = evaluate_playbook(alert, ranked)
    playbook_best = playbook_recommended_coa(legal, ranked, best_coa_id)

    state = MaritimeIncidentState(
        flow_uuid=alert.flow_uuid,
        phase=settings.phase,
        alert=alert.to_dict(),
        snapshot_bundle=bundle.to_dict(),
        best_coa_id=best_coa_id,
        playbook_best_coa_id=playbook_best,
        coa_scores=[s.to_dict() for s in ranked],
        legal_assessment=legal.to_dict(),
        agent_steps=[{"step": "legal_playbook", "status": "ok"}],
    )

    context = {
        "alert": state.alert,
        "best_coa_id": best_coa_id,
        "playbook_best_coa_id": playbook_best,
        "legal": state.legal_assessment,
        "sensor_gates": sensor_gates,
        "coa_top": state.coa_scores[:4],
    }

    narrative: str | None = None
    if settings.skip_llm or not llm_client.llm_configured():
        narrative = _template_narrative(alert, playbook_best or best_coa_id, legal)
        state.agent_steps.append({"step": "narrative_template", "status": "ok"})
    else:
        try:
            llm = llm_client.LlmSettings()
            narrative = llm_client.chat_completion_text(
                llm,
                llm_client.build_nba_messages(context),
            )
            state.llm_used = True
            state.agent_steps.append({"step": "llm_narrative", "status": "ok"})
        except Exception as exc:
            narrative = _template_narrative(alert, playbook_best or best_coa_id, legal)
            state.agent_steps.append(
                {"step": "llm_narrative", "status": f"fallback:{exc}"}
            )

    state.agent_narrative = narrative
    (artifact_dir / "agent_narrative.md").write_text(
        narrative or "", encoding="utf-8"
    )
    state.agent_steps.append({"step": "persist_state", "status": "ok"})
    persist_state(state, artifact_dir)
    return state