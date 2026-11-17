"""Automatic geography correction after primary LLM report (second pass + regex fallback)."""

from __future__ import annotations

import re
import sys

from .config import Settings
from .harbour_geography import geography_warnings
from .llm_client import chat_completion_text
from .prompts import build_geography_rewrite_prompt

_GERMAN_PRIMARILY_BALTIYSK = re.compile(
    r"German\s+Baltic\s+harbours\s*[-–—,:\s]*primarily\s+Baltiysk",
    re.IGNORECASE,
)
_GERMAN_HARBOUR_BALTIYSK = re.compile(
    r"German\s+harbours?[^\n]{0,50}Baltiysk",
    re.IGNORECASE,
)
_AT_GERMAN_HARBOR_BALTIYSK = re.compile(
    r"at\s+German\s+harbours?[^\n]{0,30}Baltiysk",
    re.IGNORECASE,
)


def rule_based_geography_fix(text: str) -> str:
    """Deterministic fixes when the LLM rewrite still mislabels Baltiysk."""
    out = text
    out = _GERMAN_PRIMARILY_BALTIYSK.sub(
        "Allied German Baltic harbours (e.g. Kiel, Rostock, Wismar) show routine-to-elevated activity; "
        "separately, adversary Russian Navy (RUS Fleet) activity is reported at Baltiysk, Russia",
        out,
    )
    out = _GERMAN_HARBOUR_BALTIYSK.sub(
        "adversary activity at the Russian naval base Baltiysk (Russia)",
        out,
    )
    out = _AT_GERMAN_HARBOR_BALTIYSK.sub("at Baltiysk, Russia (adversary naval base)", out)
    out = re.sub(
        r"within\s+[\d.,]+\s*km\s+of\s+Baltiysk,?\s+Germany",
        lambda m: m.group(0).replace("Germany", "Russia"),
        out,
        flags=re.I,
    )
    return out


def ensure_report_geography(
    settings: Settings,
    report_text: str,
    geography_context: str,
    *,
    on_llm_token=None,
) -> tuple[str, bool]:
    """
  Correlate report with harbour facts. Returns (text, was_rewritten).
  """
    warnings = geography_warnings(report_text)
    if not warnings:
        return report_text, False
    if not settings.geo_rewrite_enabled:
        for w in warnings:
            print(f"WARN: {w} (LAGEBILD_GEO_REWRITE=0 — no rewrite)", file=sys.stderr)
        return report_text, False

    print(
        f"INFO: Lagebild geography rewrite ({len(warnings)} issue(s)) …",
        file=sys.stderr,
    )
    messages = build_geography_rewrite_prompt(report_text, warnings, geography_context)
    try:
        revised = chat_completion_text(
            settings,
            messages,
            stream=bool(on_llm_token),
            on_token=on_llm_token,
        )
        if revised and len(revised.strip()) > 100:
            report_text = revised.strip()
    except Exception as exc:
        print(f"WARN: geography LLM rewrite failed: {exc}", file=sys.stderr)

    if geography_warnings(report_text):
        report_text = rule_based_geography_fix(report_text)

    still = geography_warnings(report_text)
    if still:
        for w in still:
            print(f"WARN: after rewrite: {w}", file=sys.stderr)
    else:
        print("INFO: geography rewrite OK", file=sys.stderr)
    return report_text, True