"""Harbour country / affiliation hints for Lagebild LLM grounding (reduce geo hallucination)."""

from __future__ import annotations

import re

import pandas as pd

# Subset of iceberg.defense.baltic_sea_harbours — extend as needed.
HARBOUR_COUNTRY: dict[str, str] = {
    "Kiel": "Germany",
    "Rostock": "Germany",
    "Wismar": "Germany",
    "Greifswald": "Germany",
    "Warnemünde": "Germany",
    "Eckernförde": "Germany",
    "Wilhelmshaven": "Germany",
    "Bremerhaven": "Germany",
    "Hamburg": "Germany",
    "Cuxhaven": "Germany",
    "Emden": "Germany",
    "Lübeck": "Germany",
    "Stralsund": "Germany",
    "Baltiysk": "Russia",
    "Kaliningrad": "Russia",
    "Kronstadt": "Russia",
    "Saint Petersburg": "Russia",
    "Gdynia": "Poland",
    "Gdansk": "Poland",
    "Gdańsk": "Poland",
    "Szczecin": "Poland",
    "Klaipeda": "Lithuania",
    "Klaipėda": "Lithuania",
    "Riga": "Latvia",
    "Tallinn": "Estonia",
    "Antwerp": "Belgium",
    "Helsinki": "Finland",
}

AFFILIATION_LABEL = {
    "Germany": "ALLIED",
    "Poland": "ALLIED",
    "Lithuania": "ALLIED",
    "Latvia": "ALLIED",
    "Estonia": "ALLIED",
    "Belgium": "ALLIED",
    "Finland": "ALLIED",
    "Russia": "ADVERSARY",
}


def country_for_harbour(name: str | None) -> str:
    if not name or not str(name).strip():
        return ""
    key = str(name).strip()
    if key in HARBOUR_COUNTRY:
        return HARBOUR_COUNTRY[key]
    for k, c in HARBOUR_COUNTRY.items():
        if k.lower() == key.lower():
            return c
    return "Unknown"


def affiliation_for_country(country: str) -> str:
    if not country:
        return "OFFSHORE"
    return AFFILIATION_LABEL.get(country, "NEUTRAL")


def enrich_harbour_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add harbour_country and harbour_affiliation for LLM context."""
    if df.empty or "harbour_name" not in df.columns:
        return df
    out = df.copy()
    out["harbour_country"] = out["harbour_name"].map(
        lambda v: country_for_harbour(v if pd.notna(v) else None)
    )
    out["harbour_affiliation"] = out["harbour_country"].map(affiliation_for_country)
    return out


def geography_context_block(df: pd.DataFrame) -> str:
    """Short grounding block prepended to dataset summary."""
    lines = [
        "GEOGRAPHY (mandatory — do not contradict):",
        "- Baltiysk and Kronstadt are RUSSIAN FEDERATION naval bases (Kaliningrad/Leningrad); "
        "they are NOT German harbours.",
        "- German Baltic ports include Kiel, Rostock, Wismar, Greifswald, Warnemünde, Eckernförde, etc.",
        "- Data_Source=RUS_Fleet rows describe ADVERSARY Russian Navy units; place them at their "
        "stated harbour/sector (e.g. Baltiysk, Russia), never under 'German harbours'.",
        "- Antwerp is Belgium (North Sea); not a German Baltic port.",
    ]
    if "harbour_name" in df.columns and "harbour_country" in df.columns:
        active = (
            df.dropna(subset=["harbour_name"])
            .groupby(["harbour_name", "harbour_country"], dropna=False)
            .size()
            .reset_index(name="n")
        )
        if not active.empty:
            bits = [
                f"{row.harbour_name} ({row.harbour_country or '?'})"
                for row in active.itertuples()
            ]
            lines.append(f"- Harbours in this extract: {', '.join(bits[:12])}.")
    return "\n".join(lines)


_GERMAN_BALTIYSK_RE = re.compile(
    r"German\s+Baltic\s+harbours[^\n]{0,80}Baltiysk",
    re.IGNORECASE,
)


def geography_warnings(report_text: str) -> list[str]:
    """Post-check common LLM geo errors."""
    warnings: list[str] = []
    if _GERMAN_BALTIYSK_RE.search(report_text):
        warnings.append(
            "Report text treats Baltiysk as a German Baltic harbour — "
            "Baltiysk is Russia (Kaliningrad); revise narrative."
        )
    if re.search(r"German\s+harbour[s]?[^\n]{0,40}Baltiysk", report_text, re.I):
        warnings.append("Report calls Baltiysk a German harbour.")
    return warnings