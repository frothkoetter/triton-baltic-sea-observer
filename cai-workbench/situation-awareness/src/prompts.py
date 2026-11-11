"""LLM prompt templates for NATO/Baltic C2 situation awareness."""

from __future__ import annotations

SYSTEM_SITUATION = """You are a NATO Baltic maritime situation analyst supporting C2 decision-makers.
Write clear, structured assessments in English unless the user asks otherwise.
Ground every claim in the supplied table; distinguish corroborated multi-source events from single-source hints.
Use military brevity; avoid speculation beyond the data."""


def build_situation_prompt(
    report_time: str,
    dataset_summary: str,
    data_table: str,
) -> list[dict[str, str]]:
    user = f"""--- MILITARY SITUATION REPORT ---
Analyst task: produce a situation picture (Lagebild) for German Baltic harbours from consolidated feeds.

Reporting time (analyst run): {report_time}

Dataset overview: {dataset_summary}

The table rows come from defense.lagebild (Trino: iceberg.defense.lagebild). Use Data_Source
(AIS, Buoy, LiDAR, Social_Media, Marine_Message, etc.) and details (sanctions, magnetic anomalies,
LiDAR class hints, message subjects).

Required sections (use numbered headings ``**1. …**`` or ``SECTION N: TITLE``):

1. Executive Summary — short narrative paragraph.
2. High-Priority Threat Assessment (Critical)
3. Threat Trends and Geographic Hotspots (German Baltic ports; cross-source corroboration)
4. Harbours and Anomalies
5. AIS Sanctions — explicit if any sanctioned vessel appears; otherwise state none observed
6. Recommended Actions (MPA/USV verification, sanctions follow-up, LiDAR re-scan, etc.)

Format for sections 2–5: every distinct finding MUST be its own line starting with ``* `` (asterisk
space), in the form ``* Topic: detail`` or ``* HarbourName: detail``. One bullet per line; wrap long
lines but do not start continuation lines with a new ``*``. Example:

* Sanctioned Vessel: ARISTO detected via AIS at Gdynia (Blocked under E.O. 14024).
* Greifswald: LiDAR frigate ML hint; marine feed reports operational status.

Section 6: numbered actions (1., 2., …) or ``* `` bullets. Do not prefix actions with nested
``SECTION 1:`` labels.

Data:
{data_table}

END OF DATA."""
    return [
        {"role": "system", "content": SYSTEM_SITUATION},
        {"role": "user", "content": user},
    ]


COMPARISON_SYSTEM = """You compare consecutive NATO Baltic situation reports.
Classify each thematic change as NEW, ESCALATED, DE-ESCALATED, or UNCHANGED.
Highlight critical changes and recommend next actions for C2."""


def build_comparison_prompt(
    current_report: str,
    report_24h: str | None,
    report_48h: str | None,
    report_time: str,
) -> list[dict[str, str]]:
    prior_blocks = []
    if report_24h:
        prior_blocks.append(f"--- REPORT ~24H AGO ---\n{report_24h}")
    if report_48h:
        prior_blocks.append(f"--- REPORT ~48H AGO ---\n{report_48h}")
    if not prior_blocks:
        prior_blocks.append("(No prior stored reports — note baseline only.)")

    prior_text = "\n\n".join(prior_blocks)
    user = f"""Compare the CURRENT report to prior reports for escalation analysis.

Current run time: {report_time}

{prior_text}

--- CURRENT REPORT ---
{current_report}

Output format:
## Comparison Summary
## Per-Theme Delta (prefix each line: NEW | ESCALATED | DE-ESCALATED | UNCHANGED)
## Critical Changes
## Recommended Next Actions
"""
    return [
        {"role": "system", "content": COMPARISON_SYSTEM},
        {"role": "user", "content": user},
    ]
