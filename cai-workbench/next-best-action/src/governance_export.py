"""Phase 7: PROV-O-style audit bundle for NBA incidents."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROV_CONTEXT = {
    "prov": "http://www.w3.org/ns/prov#",
    "triton": "https://triton.baltic/defense#",
}


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def artifact_manifest(artifact_dir: Path) -> list[dict[str, Any]]:
    names = (
        "audit.json",
        "decision_record.json",
        "decision_record.ttl",
        "shacl_report.json",
        "grounding.json",
        "scenario_matrix.json",
        "red_blue_replay.json",
        "what_if_red_doctrines.json",
        "materialized_tables.json",
        "incident_state.json",
        "agent_narrative.md",
    )
    manifest: list[dict[str, Any]] = []
    for name in names:
        p = artifact_dir / name
        if p.is_file():
            manifest.append(
                {
                    "name": name,
                    "sha256": _sha256_file(p),
                    "bytes": p.stat().st_size,
                }
            )
    return manifest


def build_prov_document(
    *,
    flow_uuid: str,
    decision_record: dict[str, Any],
    playbook_version: str | None,
) -> dict[str, Any]:
    generated = decision_record.get("generated_at") or datetime.now(timezone.utc).isoformat()
    activity = f"triton:activity/nba-run/{flow_uuid}"
    entity = f"triton:entity/decision-record/{flow_uuid}"
    agent = "triton:agent/nba-pipeline"
    return {
        "@context": PROV_CONTEXT,
        "id": entity,
        "type": "prov:Entity",
        "generatedAtTime": generated,
        "wasGeneratedBy": {
            "id": activity,
            "type": "prov:Activity",
            "startedAtTime": generated,
            "wasAssociatedWith": {
                "id": agent,
                "type": "prov:SoftwareAgent",
                "label": "Triton NBA Pipeline",
            },
        },
        "wasDerivedFrom": [
            {
                "id": f"triton:entity/incident/{flow_uuid}",
                "type": "prov:Entity",
                "label": "NBA incident",
            }
        ],
        "triton:playbookVersion": playbook_version,
        "triton:sections": list((decision_record.get("sections") or {}).keys()),
    }


def export_governance_bundle(
    artifact_dir: Path,
    *,
    flow_uuid: str,
    decision_record: dict[str, Any],
) -> dict[str, Any]:
    legal = (
        decision_record.get("sections", {})
        .get("4_rules_and_ontology", {})
        .get("legal_assessment", {})
    )
    playbook_version = legal.get("playbook_version")
    bundle = {
        "schema_version": "2026-11-phase7",
        "flow_uuid": flow_uuid,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "prov": build_prov_document(
            flow_uuid=flow_uuid,
            decision_record=decision_record,
            playbook_version=playbook_version,
        ),
        "artifact_manifest": artifact_manifest(artifact_dir),
        "classification": "NATO UNCLASSIFIED // TRAINING",
        "mcp_ready": {
            "iceberg_trino_mcp": "iceberg-mcp-server-trino (see .cursor/mcp.json.example)",
            "semantica_mcp": "semantica-mcp — record_decision, SHACL gate (Phase 8)",
        },
    }
    path = artifact_dir / "governance_bundle.json"
    path.write_text(json.dumps(bundle, indent=2, ensure_ascii=False), encoding="utf-8")
    prov_path = artifact_dir / "decision_record.prov.jsonld"
    prov_path.write_text(
        json.dumps(bundle["prov"], indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return bundle