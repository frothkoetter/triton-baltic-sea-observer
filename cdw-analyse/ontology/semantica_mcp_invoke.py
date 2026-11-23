#!/usr/bin/env python3
"""
Invoke Semantica MCP tools (get_graph_summary, get_business_rules) for Triton Node bridge.

Requires: pip install git+https://github.com/frothkoetter/semantica.git
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ONTOLOGY_DIR = Path(__file__).resolve().parent
REPO = ONTOLOGY_DIR.parents[1]


def _default_env() -> None:
    os.environ.setdefault(
        "SEMANTICA_KG_PATH",
        str(ONTOLOGY_DIR / "data" / "triton_defense_graph.json"),
    )
    os.environ.setdefault(
        "SEMANTICA_MAPPING_CONFIG",
        str(ONTOLOGY_DIR / "defense_r2rml_db_mapping.yaml"),
    )
    os.environ.setdefault(
        "SEMANTICA_BUSINESS_RULES",
        str(ONTOLOGY_DIR / "defense_business_rules.yaml"),
    )
    os.environ.setdefault("SEMANTICA_MCP_DISABLE_ML", "1")


def main() -> int:
    if len(sys.argv) < 2:
        print(json.dumps({"ok": False, "error": "usage: semantica_mcp_invoke.py <tool> [json-args]"}))
        return 2
    tool = sys.argv[1]
    args = json.loads(sys.argv[2]) if len(sys.argv) > 2 else {}
    _default_env()
    try:
        if tool == "get_graph_summary":
            from semantica.mcp_server import _tool_get_graph_summary

            result = _tool_get_graph_summary(args)
        elif tool == "get_business_rules":
            from semantica.mcp_server import _tool_get_business_rules

            result = _tool_get_business_rules(args)
        elif tool == "search_graph":
            from semantica.mcp_server import _tool_search_graph

            result = _tool_search_graph(args)
        else:
            result = {"ok": False, "error": f"unknown tool: {tool}"}
    except ImportError as exc:
        result = {
            "ok": False,
            "error": "semantica package not installed",
            "hint": "pip install 'git+https://github.com/frothkoetter/semantica.git'",
            "detail": str(exc),
        }
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("error") is None else 1


if __name__ == "__main__":
    raise SystemExit(main())