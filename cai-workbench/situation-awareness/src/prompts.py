"""LLM prompt templates for NATO/Baltic C2 situation awareness."""

from __future__ import annotations

SYSTEM_SITUATION = """You are a NATO Baltic maritime situation analyst supporting C2 decision-makers.
Write clear, structured assessments in English unless the user asks otherwise.
Ground every claim in the supplied table; distinguish corroborated multi-source events from single-source hints.
Use military brevity; avoid speculation beyond the data.

Geography rules (mandatory):
- **Baltiysk** and **Kronstadt** are **Russian Federation** naval bases — never describe them as German or NATO harbours.
- **German Baltic harbours** are ports in **Germany** (e.g. Kiel, Rostock, Wismar, Greifswald, Warnemünde, Eckernförde).
- **RUS_Fleet** events are **adversary** Russian Navy activity; cite the correct port/country from harbour_name and harbour_country columns.
- Do not merge adversary-base activity into an "allied harbour" narrative."""


def build_situation_prompt(
    report_time: str,
    dataset_summary: str,
    data_table: str,
) -> list[dict[str, str]]:
    user = f"""--- MILITARY SITUATION REPORT ---
Analyst task: NATO Baltic theatre Lagebild from consolidated feeds — **separate allied vs adversary geography**.

Reporting time (analyst run): {report_time}

{dataset_summary}

The table rows come from defense.lagebild (Trino: iceberg.defense.lagebild). Columns **harbour_country**
and **harbour_affiliation** (when present) are authoritative for port state (Germany/Russia/etc.).
Use Data_Source (AIS, LiDAR, RUS_Fleet, GPS_Jammer, …) and details text.

Executive summary must:
- Lead with **German / allied Baltic ports** when those rows exist.
- Describe **Baltiysk / Russian bases** only as **adversary naval activity (Russia)** — never as "German Baltic harbours".
- Never write that Russian fleet units are "at German harbours" unless harbour_country=Germany in the table for that row.

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
4. Harbours and Anomalies — **different content from section 3**: per-harbour bullets with
   ``* HarbourName (Country): detail``; include country for non-German ports; no duplicate of section 3 table.
5. AIS Sanctions — use a small table ``| Vessel | MMSI | Harbour | Status |`` when any
   sanctioned vessel appears; otherwise one sentence "No sanctioned vessels observed."
6. Conclusions & Directives — prefix each action ``DIRECTIVE 01:``, ``DIRECTIVE 02:``, … with
   imperative C2 text (MPA/USV vectoring, sanctions follow-up, LiDAR re-scan).

Use ``* Topic: detail`` bullets in section 2 and when tables are not warranted. Wrap long lines;
do not start continuation lines with a new ``*``.

Example matrix rows:
| Greifswald | LiDAR / AIS | CRITICAL | Allied (DE); LiDAR hint; AIS within 25 km. |
| Baltiysk | RUS Fleet / LiDAR | HIGH | **Russia**; adversary corvette/landing ship movements — not a German port. |

Data:
{data_table}

END OF DATA."""
    return [
        {"role": "system", "content": SYSTEM_SITUATION},
        {"role": "user", "content": user},
    ]


COMPARISON_SYSTEM = """You compare consecutive NATO Baltic situation reports.
Classify each thematic change as NEW, ESCALATED, DE-ESCALATED, or UNCHANGED.
Highlight critical changes and recommend next actions for C2.
Do not describe Baltiysk or Kronstadt as German harbours (they are Russian Federation)."""


def build_geography_rewrite_prompt(
    report_text: str,
    warnings: list[str],
    geography_context: str,
) -> list[dict[str, str]]:
    issues = "\n".join(f"- {w}" for w in warnings)
    user = f"""The following NATO Baltic SITREP draft has GEOGRAPHY ERRORS. Produce a corrected full report.

{geography_context}

ERRORS TO FIX:
{issues}

RULES:
- Baltiysk and Kronstadt are **Russia** (adversary naval bases), never German/NATO harbours.
- German Baltic ports are in **Germany** only (Kiel, Rostock, Wismar, Greifswald, …).
- Split narrative: allied German/allied sectors vs adversary activity at Russian ports.
- Keep the same section structure and headings as the draft; fix facts only; do not invent events not in the draft.
- Output the complete revised report (markdown), no preamble.

DRAFT:
{report_text}
"""
    return [
        {"role": "system", "content": SYSTEM_SITUATION},
        {"role": "user", "content": user},
    ]


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
