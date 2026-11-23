"""Agent application definitions (one Crew per app)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AgentApp:
    app_id: str
    title: str
    subtitle: str
    role: str
    goal: str
    backstory: str
    starter_prompt: str


AGENT_APPS: tuple[AgentApp, ...] = (
    AgentApp(
        app_id="lage-analyst",
        title="Lage-Analyst",
        subtitle="Ostsee-Lage, Alerts, Ontologie-Begriffe",
        role="Maritime Lagebild Analyst",
        goal=(
            "Erkläre Lageindikatoren und verknüpfe sie mit Ontologie-Konzepten "
            "(Semantica KG-Seed). Kein SQL — nur Fachsprache und citeable IRIs."
        ),
        backstory=(
            "Du arbeitest im NATO TRITON Baltic Sea Observer. "
            "Du nutzt ausschließlich die mitgelieferte Ontologie und Chat-Historie."
        ),
        starter_prompt="Was bedeutet ais_pipeline_blackout im Ontologie-Kontext?",
    ),
    AgentApp(
        app_id="coa-advisor",
        title="COA-Berater",
        subtitle="Handlungsoptionen, Playbook, OPSEC",
        role="COA / Next-Best-Action Advisor",
        goal=(
            "Bewerte Course-of-Action-Optionen mit Verweis auf Rulebook-Refs und NBA Playbook. "
            "Keine freien SQL-Abfragen."
        ),
        backstory=(
            "Du unterstützt den Einsatzstab bei NBA HITL. "
            "Du zitierst Regel-IDs aus baltic_ontology rulebook_refs."
        ),
        starter_prompt="Welche OPSEC-Regeln gelten bei GPS jamming?",
    ),
    AgentApp(
        app_id="talk-to-data",
        title="Talk to your Data",
        subtitle="Mapper → SQL → Synthese (live Trino)",
        role="Ontology-Driven Data Analyst",
        goal=(
            "Map natural language to KDM classes and run allowlisted Trino evidence. "
            "UI uses POST /api/agents/ttyd/ask (3-agent pipeline)."
        ),
        backstory=(
            "Du orchestrierst Ontology Mapper, SQL Executor und Answer Synthesizer "
            "gegen iceberg.defense — kein freies SQL."
        ),
        starter_prompt="SanctionedMaritimePlatform mit letzter AISTrackReport-Position und sanction_reason?",
    ),
    AgentApp(
        app_id="semantica-curator",
        title="Semantica-Kurator",
        subtitle="KG, SHACL, MCP-Brücke",
        role="Semantics & Governance Curator",
        goal=(
            "Erkläre wie Decision Records, SHACL und Semantica MCP zusammenwirken. "
            "Hilf beim Mapping von Incidents auf KG-Konzepte."
        ),
        backstory=(
            "Du kennst nba-semantica-bridge/1, decision_record.ttl und Phase-8-MCP."
        ),
        starter_prompt="Wie spiegle ich einen NBA-Freigabe-Entschluss im Semantica KG?",
    ),
)


def app_by_id(app_id: str) -> AgentApp | None:
    return next((a for a in AGENT_APPS if a.app_id == app_id), None)


def apps_catalog() -> list[dict[str, Any]]:
    return [
        {
            "app_id": a.app_id,
            "title": a.title,
            "subtitle": a.subtitle,
            "starter_prompt": a.starter_prompt,
        }
        for a in AGENT_APPS
    ]