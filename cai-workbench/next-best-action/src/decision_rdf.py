"""Phase 8b: decision_record + grounding → RDF/Turtle for SHACL validation."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

SHAPES_PATH = Path(__file__).resolve().parent.parent / "data" / "decision_record.ttl"
TRITON_NS = "https://triton.baltic/defense#"
NBA_NS = "https://triton.baltic/nba#"


def _slug(flow_uuid: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "_", flow_uuid)


def _expand_term(term: str | None) -> str | None:
    if not term:
        return None
    if term.startswith("http://") or term.startswith("https://"):
        return term
    if ":" in term:
        prefix, local = term.split(":", 1)
        if prefix == "triton":
            return f"{TRITON_NS}{local}"
        if prefix == "nba":
            return f"{NBA_NS}{local}"
    return term


def _turtle_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _turtle_datetime(value: str) -> str:
    """Normalize ISO-8601 to xsd:dateTime Turtle literal."""
    v = value.replace("Z", "+00:00")
    if "T" in v and "+" not in v[10:]:
        v = f"{v}+00:00"
    return f'"{v}"^^xsd:dateTime'


def decision_record_to_ttl(
    decision_record: dict[str, Any],
    grounding: dict[str, Any] | None,
) -> str:
    flow = str(decision_record.get("flow_uuid") or "")
    slug = _slug(flow)
    generated = str(decision_record.get("generated_at") or "")
    sections = decision_record.get("sections") or {}
    ranked = (sections.get("3_ranked_coas") or {}).get("coa_scores") or []
    best = (sections.get("3_ranked_coas") or {}).get("best_coa_id")
    if not best and ranked:
        for row in ranked:
            if row.get("gates_passed"):
                best = row.get("coa_id") or row.get("coa", {}).get("coa_id")
                break
        if not best:
            row0 = ranked[0]
            best = row0.get("coa_id") or row0.get("coa", {}).get("coa_id")
    playbook_best = (sections.get("3_ranked_coas") or {}).get("playbook_best_coa_id")
    g = grounding or (sections.get("4_rules_and_ontology") or {}).get("grounding") or {}

    lines = [
        "@prefix nba: <https://triton.baltic/nba#> .",
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
        "",
        f"nba:decision_{slug} a nba:IncidentDecision ;",
        f"    nba:flowUuid {_turtle_string(flow)} ;",
        f"    nba:generatedAt {_turtle_datetime(generated)} ;",
    ]
    if best:
        lines.append(f"    nba:bestCoaId {_turtle_string(str(best))} ;")
    if playbook_best:
        lines.append(
            f"    nba:playbookBestCoaId {_turtle_string(str(playbook_best))} ;"
        )
    lines.append(f"    nba:hasGrounding nba:grounding_{slug} .")
    lines.append("")

    g_lines = [f"nba:grounding_{slug} a nba:IncidentGrounding ;"]
    if g.get("ontology_version"):
        g_lines.append(
            f"    nba:ontologyVersion {_turtle_string(str(g['ontology_version']))} ;"
        )
    concept = _expand_term(g.get("incident_concept"))
    if concept:
        if concept.startswith("http"):
            g_lines.append(f"    nba:incidentConcept <{concept}> ;")
        else:
            g_lines.append(f"    nba:incidentConcept {_turtle_string(concept)} ;")

    cites = list(g.get("rule_citations") or [])
    if not cites:
        for ref in g.get("rulebook_refs") or []:
            rid = ref.get("ref") or ref.get("label") or "rulebook"
            cites.append({"rule_id": str(rid)})
    cite_blocks: list[str] = []
    for i, cite in enumerate(cites):
        rid = cite.get("rule_id") or f"rule-{i}"
        cslug = _slug(f"{slug}-{rid}")
        cite_blocks.extend(
            [
                f"nba:citation_{cslug} a nba:RuleCitation ;",
                f"    nba:ruleId {_turtle_string(str(rid))} .",
                "",
            ]
        )
        g_lines.append(f"    nba:hasRuleCitation nba:citation_{cslug} ;")

    if g_lines[-1].endswith(" ;"):
        g_lines[-1] = g_lines[-1][:-2] + " ."
    else:
        g_lines[-1] = g_lines[-1] + " ."

    lines.extend(g_lines)
    lines.append("")
    lines.extend(cite_blocks)
    return "\n".join(lines) + "\n"