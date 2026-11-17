"""Persist MaritimeIncidentState under artifacts/nba/<flow_uuid>/."""

from __future__ import annotations

import json
from pathlib import Path

from .maritime_incident_state import MaritimeIncidentState


def persist_state(state: MaritimeIncidentState, artifact_dir: Path) -> Path:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    path = artifact_dir / "incident_state.json"
    state.touch()
    path.write_text(
        state.model_dump_json(indent=2),
        encoding="utf-8",
    )
    history = artifact_dir / "state_history"
    history.mkdir(exist_ok=True)
    stamp = state.updated_at.strftime("%Y%m%dT%H%M%S")
    (history / f"state_{stamp}.json").write_text(
        path.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    return path


def load_state(artifact_dir: Path) -> MaritimeIncidentState | None:
    path = artifact_dir / "incident_state.json"
    if not path.is_file():
        return None
    return MaritimeIncidentState.model_validate_json(path.read_text(encoding="utf-8"))