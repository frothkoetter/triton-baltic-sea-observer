"""Phase 8.3: artifact bridge for Semantica MCP (record_decision, precedents)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BRIDGE_SCHEMA = "nba-semantica-bridge/1"


def build_semantica_record(
    *,
    flow_uuid: str,
    decision: dict[str, Any],
    grounding: dict[str, Any] | None,
    shacl: dict[str, Any] | None = None,
    snapshot_capture_source: str | None = None,
    governance_exported: bool = False,
) -> dict[str, Any]:
    sections = decision.get("sections") or {}
    ranked = (sections.get("3_ranked_coas") or {})
    what = sections.get("1_what_happened") or {}
    g = grounding or (sections.get("4_rules_and_ontology") or {}).get("grounding") or {}

    return {
        "schema": BRIDGE_SCHEMA,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "flow_uuid": flow_uuid,
        "decision": {
            "generated_at": decision.get("generated_at"),
            "theatre": what.get("theatre"),
            "alert_type": what.get("alert_type"),
            "priority": what.get("priority"),
            "best_coa_id": ranked.get("best_coa_id"),
            "playbook_best_coa_id": ranked.get("playbook_best_coa_id"),
            "incident_concept": g.get("incident_concept"),
            "incident_concept_label": g.get("incident_concept_label"),
            "ontology_version": g.get("ontology_version"),
        },
        "compliance": {
            "shacl_conforms": shacl.get("conforms") if shacl else None,
            "shacl_engine": shacl.get("engine") if shacl else None,
        },
        "snapshots": {"capture_source": snapshot_capture_source},
        "artifacts": {
            "decision_record_json": "decision_record.json",
            "decision_record_ttl": "decision_record.ttl",
            "grounding_json": "grounding.json",
            "shacl_report_json": "shacl_report.json",
            "governance_bundle_json": (
                "governance_bundle.json" if governance_exported else None
            ),
        },
        "mcp_hints": {
            "semantica_record_decision": (
                "MCP: record_decision with flow_uuid, best_coa_id, incident_concept"
            ),
            "semantica_query_precedents": (
                "MCP: query_decisions filtered by incident_concept / theatre"
            ),
            "shacl_shapes_repo_path": "cai-workbench/next-best-action/data/decision_record.ttl",
            "kg_seed_path": "cai-workbench/next-best-action/data/baltic_ontology.json",
        },
    }


def write_semantica_record(
    artifact_dir: Path,
    payload: dict[str, Any],
) -> Path:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    path = artifact_dir / "semantica_record.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path