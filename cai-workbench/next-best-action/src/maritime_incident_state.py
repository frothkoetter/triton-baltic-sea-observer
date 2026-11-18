"""Pydantic incident state for Phase 3 agent flow (@persist target)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


class MaritimeIncidentState(BaseModel):
    flow_uuid: str
    phase: int = 3
    alert: dict[str, Any]
    snapshot_bundle: dict[str, Any] | None = None
    best_coa_id: str | None = None
    playbook_best_coa_id: str | None = None
    coa_scores: list[dict[str, Any]] = Field(default_factory=list)
    legal_assessment: dict[str, Any] = Field(default_factory=dict)
    grounding: dict[str, Any] = Field(default_factory=dict)
    scenario_runs: list[dict[str, Any]] = Field(default_factory=list)
    agentic_gameplay: dict[str, Any] | None = None
    decision_record: dict[str, Any] | None = None
    shacl_report: dict[str, Any] | None = None
    agent_narrative: str | None = None
    agent_steps: list[dict[str, str]] = Field(default_factory=list)
    lagebild_pin_note: str = (
        "Lagebild view is live-only; fact tables pinned via snapshot bundle."
    )
    llm_used: bool = False
    persist_backend: str = "nba_json"
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {"extra": "ignore"}

    def touch(self) -> None:
        self.updated_at = datetime.now(timezone.utc)