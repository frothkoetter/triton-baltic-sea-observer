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
2. High-Priority Threat Assessment (Critical) — open with one bold callout line
   (``**CRITICAL CONTACT: …**``) then ``* Topic: detail`` bullets for each priority item.
3. Threat Trends and Geographic Hotspots — include a **markdown pipe table** titled
   "Geographic Hotspots & Anomaly Matrix" with columns exactly:
   ``| Sector / Port | Data Source | Threat Level | Tactical Assessment |``
   One row per active harbour/sector (sorted by Threat Level: CRITICAL, then HIGH/MEDIUM, then LOW).
   Threat Level must be one of: CRITICAL, HIGH, MEDIUM, LOW. After the table you may add brief bullets
   only for cross-source items that do not fit a single row.
4. Harbours and Anomalies — prefer the same 4-column table when ≥3 harbours have events;
   otherwise one ``* HarbourName: detail`` bullet per harbour (no duplicate harbour names).
5. AIS Sanctions — use a small table ``| Vessel | MMSI | Harbour | Status |`` when any
   sanctioned vessel appears; otherwise one sentence "No sanctioned vessels observed."
6. Conclusions & Directives — prefix each action ``DIRECTIVE 01:``, ``DIRECTIVE 02:``, … with
   imperative C2 text (MPA/USV vectoring, sanctions follow-up, LiDAR re-scan).

Use ``* Topic: detail`` bullets in section 2 and when tables are not warranted. Wrap long lines;
do not start continuation lines with a new ``*``.

Example matrix row:
| Greifswald | LiDAR / AIS | CRITICAL | LiDAR frigate hint; sanctioned AIS track within 25 km. |

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
