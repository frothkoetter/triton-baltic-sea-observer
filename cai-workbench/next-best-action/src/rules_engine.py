"""Deterministic MC-362 / UNCLOS-style gates (no LLM)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .alert_types import NbaAlert
from .coa_catalog import COA_CATALOG, CoaTemplate
from .scoring import CoaScore

_PLAYBOOK_PATH = Path(__file__).resolve().parent.parent / "data" / "rules_playbook.json"


@dataclass
class LegalAssessment:
    playbook_version: str
    applicable_rule_ids: list[str]
    messages: list[str]
    coa_flags_blocked: dict[str, list[str]] = field(default_factory=dict)
    approved_coa_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "playbook_version": self.playbook_version,
            "applicable_rule_ids": self.applicable_rule_ids,
            "messages": self.messages,
            "coa_flags_blocked": self.coa_flags_blocked,
            "approved_coa_ids": self.approved_coa_ids,
        }


def _load_playbook() -> dict[str, Any]:
    if not _PLAYBOOK_PATH.is_file():
        return {"version": "missing", "alert_rules": [], "coa_legal_notes": {}}
    return json.loads(_PLAYBOOK_PATH.read_text(encoding="utf-8"))


def _coa_by_id(coa_id: str) -> CoaTemplate | None:
    for c in COA_CATALOG:
        if c.coa_id == coa_id:
            return c
    return None


def _coa_has_flag(coa: CoaTemplate, flag: str) -> bool:
    return bool(getattr(coa, flag, False))


def evaluate_playbook(
    alert: NbaAlert,
    ranked: list[CoaScore],
    *,
    best_discovery: float | None = None,
) -> LegalAssessment:
    pb = _load_playbook()
    rule_ids: list[str] = []
    messages: list[str] = []
    blocked: dict[str, list[str]] = {}
    approved: list[str] = []

    discovery = best_discovery
    if discovery is None:
        for s in ranked:
            if s.gates_passed:
                discovery = max(discovery or 0.0, s.score_entdeckung)
                break

    for rule in pb.get("alert_rules", []):
        if alert.alert_type not in rule.get("alert_types", []):
            continue
        rule_ids.append(rule["id"])
        messages.append(rule.get("message", rule["id"]))
        unless_flash = rule.get("unless_priority") == "FLASH" and alert.priority == "FLASH"
        unless_disc = rule.get("unless_best_discovery_below")
        skip_block = unless_flash or (
            unless_disc is not None
            and discovery is not None
            and discovery < float(unless_disc)
        )
        if skip_block:
            continue
        for flag in rule.get("disallow_coa_flags", []):
            for score in ranked:
                if not score.gates_passed:
                    continue
                if _coa_has_flag(score.coa, flag):
                    blocked.setdefault(score.coa.coa_id, []).append(
                        f"{rule['id']}:blocked_{flag}"
                    )

    legal_notes = pb.get("coa_legal_notes", {})
    for score in ranked:
        if not score.gates_passed:
            continue
        cid = score.coa.coa_id
        if cid in blocked:
            continue
        approved.append(cid)
        if cid in legal_notes:
            messages.append(f"COA {cid}: {legal_notes[cid]}")

    return LegalAssessment(
        playbook_version=str(pb.get("version", "")),
        applicable_rule_ids=rule_ids,
        messages=messages,
        coa_flags_blocked=blocked,
        approved_coa_ids=approved,
    )


def playbook_recommended_coa(
    legal: LegalAssessment,
    ranked: list[CoaScore],
    deterministic_best: str | None,
) -> str | None:
    """Prefer deterministic best if still legally approved; else top approved rank."""
    if deterministic_best and deterministic_best in legal.approved_coa_ids:
        return deterministic_best
    for score in ranked:
        if score.gates_passed and score.coa.coa_id in legal.approved_coa_ids:
            return score.coa.coa_id
    return deterministic_best