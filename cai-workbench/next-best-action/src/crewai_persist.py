"""Phase 8.4: optional CrewAI @persist bridge (defaults to NBA JSON state)."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .agent_orchestrator import run_orchestrator_closure
from .maritime_incident_state import MaritimeIncidentState
from .state_persist import persist_state

MANIFEST_SCHEMA = "nba-crewai-persist/1"
BACKEND_JSON = "nba_json"
BACKEND_CREWAI = "crewai"


def _crewai_import_error() -> str | None:
    if (sys.version_info.major, sys.version_info.minor) < (3, 10):
        return "Python >= 3.10 required"
    try:
        import crewai  # noqa: F401
    except ImportError as exc:
        return str(exc)
    return None


def _crewai_persist_touch(state: MaritimeIncidentState) -> MaritimeIncidentState:
    """Minimal CrewAI hook when package is installed (full Flow later)."""
    import crewai  # noqa: F401

    state.agent_steps.append(
        {
            "step": "crewai_persist_bridge",
            "status": "ok",
            "agent": "CrewAIPersist",
            "summary": "CrewAI package present; state mirrored to incident_state.json.",
        }
    )
    return state


def write_persist_manifest(
    artifact_dir: Path,
    *,
    flow_uuid: str,
    backend: str,
    crewai_error: str | None,
) -> Path:
    manifest: dict[str, Any] = {
        "schema": MANIFEST_SCHEMA,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "flow_uuid": flow_uuid,
        "persist_backend": backend,
        "state_path": "incident_state.json",
        "history_dir": "state_history/",
        "crewai_package_error": crewai_error,
        "notes": (
            "Default CML path: nba_json. Install requirements-crewai.txt and "
            "NBA_CREWAI_PERSIST=1 for crewai backend."
        ),
    }
    artifact_dir.mkdir(parents=True, exist_ok=True)
    path = artifact_dir / "crewai_persist_manifest.json"
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return path


def run_orchestrator_with_persist(
    state: MaritimeIncidentState,
    artifact_dir: Path,
    *,
    crewai_persist: bool,
    simulations: list[dict[str, Any]] | None,
    decision_record: dict[str, Any],
) -> MaritimeIncidentState:
    state = run_orchestrator_closure(
        state,
        artifact_dir,
        simulations=simulations,
        decision_record=decision_record,
    )
    backend = BACKEND_JSON
    crew_error: str | None = None
    if crewai_persist:
        crew_error = _crewai_import_error()
        if crew_error is None:
            try:
                state = _crewai_persist_touch(state)
                backend = BACKEND_CREWAI
            except Exception as exc:  # noqa: BLE001
                crew_error = str(exc)
                state.agent_steps.append(
                    {
                        "step": "crewai_persist_bridge",
                        "status": f"fallback:{exc}",
                        "agent": "CrewAIPersist",
                    }
                )
    state.persist_backend = backend
    persist_state(state, artifact_dir)
    write_persist_manifest(
        artifact_dir,
        flow_uuid=state.flow_uuid,
        backend=backend,
        crewai_error=crew_error,
    )
    return state