"""CrewAI chat runtime with LLM and offline fallbacks."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from .apps import AgentApp, app_by_id
from .ontology import format_ontology_context, load_ontology

_NBA_SRC = Path(__file__).resolve().parents[2] / "next-best-action"
if _NBA_SRC.is_dir() and str(_NBA_SRC) not in sys.path:
    sys.path.insert(0, str(_NBA_SRC))


def _history_text(messages: list[dict[str, str]], limit: int = 12) -> str:
    tail = messages[-limit:]
    lines = []
    for m in tail:
        role = m.get("role", "user")
        content = (m.get("content") or "").strip()
        lines.append(f"{role.upper()}: {content}")
    return "\n".join(lines)


def _offline_reply(app: AgentApp, messages: list[dict[str, str]], onto_ctx: str) -> str:
    last = ""
    for m in reversed(messages):
        if m.get("role") == "user":
            last = (m.get("content") or "").strip()
            break
    onto = load_ontology()
    concepts = onto.get("alert_types") or {}
    rules = onto.get("rulebook_refs") or {}
    hint = ""
    for key, meta in concepts.items():
        if key.lower() in last.lower() or (meta.get("label") or "").lower() in last.lower():
            hint = f"Konzept `{key}` → IRI {meta.get('iri')}: {meta.get('label')}."
            break
    if not hint and ("opsec" in last.lower() or "gps" in last.lower()):
        gps = rules.get("NATO:MC362-2§collective-opsec", {})
        hint = f"Rulebook {gps.get('label', 'MC 362/2')}: {gps.get('summary', '')}"
    if not hint and "semantica" in last.lower():
        hint = (
            "Semantica: `record_decision` mit flow_uuid, incident_concept aus §4 grounding; "
            "SHACL-Gate vor Trino-Persist (NBA Phase 8)."
        )
    if not hint:
        hint = f"({app.title}) Nutze Ontologie-Begriffe statt SQL — siehe Seitenleiste."
    return (
        f"**{app.title}** (Offline / kein CrewAI-LLM)\n\n"
        f"Ihre Frage: {last or '—'}\n\n"
        f"{hint}\n\n"
        f"Ontologie-Auszug:\n{onto_ctx[:800]}"
    )


def _llm_reply(app: AgentApp, messages: list[dict[str, str]], onto_ctx: str) -> str | None:
    mode = os.environ.get("TRITON_AGENT_MODE", "auto").lower()
    if mode == "offline":
        return None
    try:
        from src.llm_client import LlmSettings, chat_completion_text, llm_configured
    except ImportError:
        return None
    if not llm_configured():
        return None
    llm = LlmSettings()
    system = (
        f"{app.role}. {app.goal}\n{app.backstory}\n\n"
        f"Ontologie (Semantica seed):\n{onto_ctx}\n\n"
        "Antworte auf Deutsch, präzise, max. 2 kurze Absätze. Kein SQL."
    )
    convo = _history_text(messages)
    out_messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": convo},
    ]
    return chat_completion_text(llm, out_messages)


def _crewai_reply(app: AgentApp, messages: list[dict[str, str]], onto_ctx: str) -> str | None:
    mode = os.environ.get("TRITON_AGENT_MODE", "auto").lower()
    if mode in ("offline", "llm"):
        return None
    try:
        from crewai import Agent, Crew, Process, Task
    except ImportError:
        return None
    last_user = ""
    for m in reversed(messages):
        if m.get("role") == "user":
            last_user = m.get("content") or ""
            break
    analyst = Agent(
        role=app.role,
        goal=app.goal,
        backstory=app.backstory,
        verbose=False,
        allow_delegation=False,
    )
    desc = (
        f"Ontologie:\n{onto_ctx}\n\n"
        f"Konversation:\n{_history_text(messages)}\n\n"
        f"Beantworte die letzte USER-Nachricht auf Deutsch (kein SQL)."
    )
    task = Task(
        description=desc,
        expected_output="Zwei kurze Absätze auf Deutsch mit Ontologie-/Rulebook-Bezug.",
        agent=analyst,
    )
    crew = Crew(
        agents=[analyst],
        tasks=[task],
        process=Process.sequential,
        verbose=False,
    )
    result = crew.kickoff()
    text = str(result).strip()
    if not text and last_user:
        return None
    return text


def run_agent_chat(
    *,
    app_id: str,
    messages: list[dict[str, str]],
) -> dict[str, Any]:
    app = app_by_id(app_id)
    if app is None:
        return {"ok": False, "error": f"Unknown app_id: {app_id}"}
    onto_ctx = format_ontology_context()
    engine = "offline"
    reply: str | None = None
    crew_err: str | None = None
    try:
        reply = _crewai_reply(app, messages, onto_ctx)
        if reply:
            engine = "crewai"
    except Exception as exc:  # noqa: BLE001
        crew_err = str(exc)
        reply = None
    if not reply:
        try:
            reply = _llm_reply(app, messages, onto_ctx)
            if reply:
                engine = "llm"
        except Exception as exc:  # noqa: BLE001
            crew_err = crew_err or str(exc)
            reply = None
    if not reply:
        reply = _offline_reply(app, messages, onto_ctx)
        engine = "offline"
    return {
        "ok": True,
        "app_id": app_id,
        "engine": engine,
        "message": {"role": "assistant", "content": reply},
        "ontology_namespace": load_ontology().get("namespace"),
        "crewai_error": crew_err,
    }