"""NATO-aligned business rules + Trino evidence mapping for agents."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_DATA = Path(__file__).resolve().parents[2] / "next-best-action" / "data"


def _path(name: str, override_env: str) -> Path:
    import os

    raw = os.environ.get(override_env)
    if raw:
        return Path(raw)
    return _DATA / name


def load_business_rules() -> dict[str, Any]:
    path = _path("baltic_business_rules.json", "TRITON_BUSINESS_RULES_PATH")
    if not path.is_file():
        return {"version": "missing", "business_rules": [], "nato_public_sources": []}
    return json.loads(path.read_text(encoding="utf-8"))


def load_trino_mapping() -> dict[str, Any]:
    path = _path("baltic_trino_mapping.json", "TRITON_TRINO_MAPPING_PATH")
    if not path.is_file():
        return {"version": "missing", "semantic_entities": [], "alert_type_bindings": {}}
    return json.loads(path.read_text(encoding="utf-8"))


def business_rules_summary() -> dict[str, Any]:
    br = load_business_rules()
    tm = load_trino_mapping()
    rules = br.get("business_rules") or []
    return {
        "version": br.get("version"),
        "classification_note": br.get("classification_note"),
        "nato_public_sources": br.get("nato_public_sources") or [],
        "business_rules": [
            {
                "rule_id": r.get("rule_id"),
                "title": r.get("title"),
                "source_ids": r.get("source_ids"),
                "ontology_alert_types": r.get("ontology_alert_types"),
                "playbook_rule_id": r.get("playbook_rule_id"),
                "tables": (r.get("trino_bindings") or {}).get("tables"),
                "entities": (r.get("trino_bindings") or {}).get("entities"),
            }
            for r in rules
        ],
        "trino_mapping": {
            "version": tm.get("version"),
            "warehouse": tm.get("warehouse"),
            "entity_count": len(tm.get("semantic_entities") or []),
            "semantic_entities": [
                {"entity_id": e.get("entity_id"), "iri": e.get("iri"), "tables": e.get("tables")}
                for e in (tm.get("semantic_entities") or [])
            ],
            "alert_type_bindings": tm.get("alert_type_bindings"),
        },
    }


def rules_for_alert_type(alert_type: str) -> list[dict[str, Any]]:
    br = load_business_rules()
    out = []
    for r in br.get("business_rules") or []:
        if alert_type in (r.get("ontology_alert_types") or []):
            out.append(r)
    return out


def format_rules_context(max_rules: int = 8) -> str:
    summary = business_rules_summary()
    lines = [
        f"Business rules catalog {summary.get('version')} — {summary.get('classification_note', '')}",
        "Rules (NATO public paraphrase → Trino evidence):",
    ]
    for r in (summary.get("business_rules") or [])[:max_rules]:
        tables = ", ".join(r.get("tables") or [])
        lines.append(
            f"  - {r.get('rule_id')}: {r.get('title')} | tables: {tables} | sources: {','.join(r.get('source_ids') or [])}"
        )
    wh = (summary.get("trino_mapping") or {}).get("warehouse") or {}
    if wh:
        lines.append(f"Warehouse: {wh.get('catalog')}.{wh.get('schema')}")
    return "\n".join(lines)