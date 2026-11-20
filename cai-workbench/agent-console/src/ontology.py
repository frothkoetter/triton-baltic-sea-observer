"""Load Baltic Semantica seed ontology for agent grounding."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_REPO_ONTOLOGY = (
    Path(__file__).resolve().parents[2]
    / "next-best-action"
    / "data"
    / "baltic_ontology.json"
)


def ontology_path() -> Path:
    import os

    override = os.environ.get("TRITON_ONTOLOGY_PATH")
    if override:
        return Path(override)
    return _REPO_ONTOLOGY


def load_ontology() -> dict[str, Any]:
    path = ontology_path()
    if not path.is_file():
        return {"version": "missing", "namespace": "", "alert_types": {}, "rulebook_refs": {}}
    return json.loads(path.read_text(encoding="utf-8"))


def ontology_summary(onto: dict[str, Any] | None = None) -> dict[str, Any]:
    data = onto or load_ontology()
    concepts = [
        {
            "id": key,
            "iri": meta.get("iri"),
            "label": meta.get("label"),
            "emodnet_hint": meta.get("emodnet_hint"),
        }
        for key, meta in (data.get("alert_types") or {}).items()
    ]
    rules = [
        {"id": key, **(meta if isinstance(meta, dict) else {})}
        for key, meta in (data.get("rulebook_refs") or {}).items()
    ]
    return {
        "version": data.get("version"),
        "namespace": data.get("namespace"),
        "concepts": concepts,
        "rules": rules,
        "semantica": {
            "kg_seed_path": str(ontology_path()),
            "mcp_hints": [
                "record_decision / query_decisions (Semantica MCP)",
                "export_graph from baltic_ontology.json → RDF",
                "SHACL: decision_record.ttl (NBA Phase 8)",
            ],
        },
    }


def format_ontology_context(onto: dict[str, Any] | None = None, max_rules: int = 6) -> str:
    summary = ontology_summary(onto)
    lines = [
        f"Ontology {summary.get('version')} · {summary.get('namespace')}",
        "Incident concepts:",
    ]
    for c in summary.get("concepts", [])[:12]:
        lines.append(f"  - {c['id']}: {c.get('label')} ({c.get('iri', '')})")
    lines.append("Rulebook refs:")
    for r in summary.get("rules", [])[:max_rules]:
        lines.append(f"  - {r['id']}: {r.get('label')}")
    return "\n".join(lines)