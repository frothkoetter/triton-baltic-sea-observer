"""Python entry: 3-agent TTYD (delegates mapping; UI runs SQL via Node)."""

from __future__ import annotations

from typing import Any

from .mapping_plan import build_mapping_plan, resolve_golden_example


def run_ttyd_agents(user_text: str) -> dict[str, Any]:
    resolved = resolve_golden_example(user_text)
    if not resolved.get("ok"):
        return {
            "ok": False,
            "agents": {
                "ontology_mapper": {
                    "ok": False,
                    "error": resolved.get("error"),
                }
            },
            "hint": "POST /api/agents/ttyd/ask für vollständige SQL+Synthese",
        }
    ex = resolved["example"]
    plan = build_mapping_plan(ex)
    return {
        "ok": True,
        "agents": {
            "ontology_mapper": {
                "ok": True,
                "example_id": ex.get("id"),
                "mapping_plan": plan,
            },
            "sql_executor": {
                "ok": None,
                "note": "Execute via NATO UI POST /api/agents/ttyd/ask or kg-sql",
                "kg_query_id": ex.get("kg_query_id"),
            },
            "answer_synthesizer": {"ok": None, "note": "Markdown produced server-side"},
        },
    }