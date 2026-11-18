"""Phase 7 multi-agent steps (CrewAI-ready; deterministic today)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .maritime_incident_state import MaritimeIncidentState
from .state_persist import persist_state


def _sim_analyst_summary(simulations: list[dict[str, Any]] | None) -> str:
    if not simulations:
        return "No simulation traces."
    parts = [
        f"COA {s.get('coa_id')}: discovery={s.get('final_discovery')}"
        for s in simulations[:4]
    ]
    return "Sim analyst: " + "; ".join(parts)


def run_orchestrator_closure(
    state: MaritimeIncidentState,
    artifact_dir: Path,
    *,
    simulations: list[dict[str, Any]] | None,
    decision_record: dict[str, Any],
) -> MaritimeIncidentState:
    """Append specialist agent steps after legal + narrative (persist target for CrewAI)."""
    state.agent_steps.append(
        {
            "step": "sim_analyst_agent",
            "status": "ok",
            "agent": "SimAnalyst",
            "summary": _sim_analyst_summary(simulations),
        }
    )
    sections = len((decision_record.get("sections") or {}))
    state.agent_steps.append(
        {
            "step": "governance_seal_agent",
            "status": "ok",
            "agent": "GovSeal",
            "summary": f"Decision record sealed with {sections} sections.",
        }
    )
    persist_state(state, artifact_dir)
    return state