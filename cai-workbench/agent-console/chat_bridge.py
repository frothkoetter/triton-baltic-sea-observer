#!/usr/bin/env python3
"""JSON stdin/stdout bridge for NATO UI agent chat."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.crew_runtime import run_agent_chat  # noqa: E402
from src.business_rules import business_rules_summary  # noqa: E402
from src.ontology import ontology_summary  # noqa: E402


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        json.dump({"ok": False, "error": "Invalid JSON stdin"}, sys.stdout)
        return 1
    action = payload.get("action", "chat")
    if action == "ontology":
        json.dump({"ok": True, "ontology": ontology_summary()}, sys.stdout, ensure_ascii=False)
        return 0
    if action == "business_rules":
        json.dump({"ok": True, **business_rules_summary()}, sys.stdout, ensure_ascii=False)
        return 0
    if action == "chat":
        app_id = str(payload.get("app_id") or "lage-analyst")
        messages = payload.get("messages") or []
        if not isinstance(messages, list):
            json.dump({"ok": False, "error": "messages must be a list"}, sys.stdout)
            return 1
        result = run_agent_chat(app_id=app_id, messages=messages)
        json.dump(result, sys.stdout, ensure_ascii=False)
        return 0 if result.get("ok") else 1
    json.dump({"ok": False, "error": f"Unknown action: {action}"}, sys.stdout)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())