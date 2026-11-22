"""Ontology and rulebook grounding for NBA incidents (deterministic, no LLM)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .alert_types import NbaAlert
from .rules_engine import LegalAssessment
from .scoring import CoaScore

_ONTOLOGY_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "baltic_ontology.json"
)
_PLAYBOOK_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "rules_playbook.json"
)
_BUSINESS_RULES_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "baltic_business_rules.json"
)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _rule_citations(rule_ids: list[str]) -> list[dict[str, Any]]:
    pb = _load_json(_PLAYBOOK_PATH)
    by_id = {r["id"]: r for r in pb.get("alert_rules", [])}
    cites: list[dict[str, Any]] = []
    for rid in rule_ids:
        rule = by_id.get(rid, {})
        cites.append(
            {
                "rule_id": rid,
                "message": rule.get("message", rid),
                "ontology_term": rule.get("ontology_term"),
                "rulebook_ref": rule.get("rulebook_ref"),
                "full_text": rule.get(
                    "full_text", rule.get("message", "No extended rule text.")
                ),
            }
        )
    return cites


@dataclass
class IncidentGrounding:
    ontology_version: str
    incident_concept: str | None
    incident_concept_label: str | None
    alert_type_iri: str | None
    rulebook_refs: list[dict[str, str]]
    rule_citations: list[dict[str, Any]]
    coa_justifications: list[dict[str, Any]] = field(default_factory=list)
    business_rule_bindings: list[dict[str, Any]] = field(default_factory=list)
    llm_grounding_mode: str = "narrative-only"

    def to_dict(self) -> dict[str, Any]:
        return {
            "ontology_version": self.ontology_version,
            "incident_concept": self.incident_concept,
            "incident_concept_label": self.incident_concept_label,
            "alert_type_iri": self.alert_type_iri,
            "rulebook_refs": self.rulebook_refs,
            "rule_citations": self.rule_citations,
            "business_rule_bindings": self.business_rule_bindings,
            "coa_justifications": self.coa_justifications,
            "llm_grounding_mode": self.llm_grounding_mode,
        }


def _business_rule_bindings(alert_type: str) -> list[dict[str, Any]]:
    br = _load_json(_BUSINESS_RULES_PATH)
    bindings: list[dict[str, Any]] = []
    for rule in br.get("business_rules", []):
        if alert_type not in rule.get("ontology_alert_types", []):
            continue
        tb = rule.get("trino_bindings") or {}
        bindings.append(
            {
                "rule_id": rule.get("rule_id"),
                "title": rule.get("title"),
                "source_ids": rule.get("source_ids"),
                "playbook_rule_id": rule.get("playbook_rule_id"),
                "trino_tables": tb.get("tables"),
                "trino_entities": tb.get("entities"),
                "evidence_template": tb.get("evidence_template"),
            }
        )
    return bindings


def build_grounding(
    alert: NbaAlert,
    legal: LegalAssessment,
    ranked: list[CoaScore],
    *,
    playbook_best: str | None,
    best_coa_id: str | None,
    sensor_gates: dict[str, Any] | None = None,
) -> IncidentGrounding:
    onto = _load_json(_ONTOLOGY_PATH)
    pb = _load_json(_PLAYBOOK_PATH)
    alert_map = onto.get("alert_types", {})
    at = alert_map.get(alert.alert_type, {})
    rulebook_keys = pb.get("references", [])
    rb_map = onto.get("rulebook_refs", {})

    rulebook_refs: list[dict[str, str]] = []
    for key in rulebook_keys:
        if isinstance(key, str) and key in rb_map:
            rulebook_refs.append({"ref": key, **rb_map[key]})
        elif isinstance(key, str):
            rulebook_refs.append({"ref": key, "label": key, "summary": ""})

    citations = _rule_citations(legal.applicable_rule_ids)

    justifications: list[dict[str, Any]] = []
    rank = 0
    for score in ranked:
        cid = score.coa.coa_id
        rules_for_coa: list[str] = list(legal.applicable_rule_ids)
        blocked = legal.coa_flags_blocked.get(cid, [])
        if blocked:
            rules_for_coa = [f"blocked:{b}" for b in blocked]
        if score.gates_passed:
            rank += 1
        justifications.append(
            {
                "coa_id": cid,
                "rank": rank if score.gates_passed else None,
                "gates_passed": score.gates_passed,
                "gate_fail_reasons": score.gate_fail_reasons,
                "rules": rules_for_coa,
                "legal_note": pb.get("coa_legal_notes", {}).get(cid),
                "recommended": cid
                in {
                    playbook_best,
                    best_coa_id,
                }
                and cid in legal.approved_coa_ids,
            }
        )

    ontology_version = str(onto.get("version") or "unknown")
    incident_concept = at.get("iri")
    incident_concept_label = at.get("label")
    if not incident_concept:
        slug = alert.alert_type.replace("-", "_")
        incident_concept = f"triton:{''.join(p.capitalize() for p in slug.split('_'))}Event"
        incident_concept_label = incident_concept_label or (
            f"NBA alert type `{alert.alert_type}` (ontology fallback)"
        )

    return IncidentGrounding(
        ontology_version=ontology_version,
        incident_concept=incident_concept,
        incident_concept_label=incident_concept_label,
        alert_type_iri=at.get("iri") or incident_concept,
        rulebook_refs=rulebook_refs,
        rule_citations=citations,
        business_rule_bindings=_business_rule_bindings(alert.alert_type),
        coa_justifications=justifications,
        llm_grounding_mode="narrative-only",
    )