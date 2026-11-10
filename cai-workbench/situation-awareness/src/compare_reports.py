"""Parse comparison output and rule-based diffs."""

from __future__ import annotations

import re
from dataclasses import dataclass

DELTA_PREFIXES = ("NEW", "ESCALATED", "DE-ESCALATED", "UNCHANGED")
_LINE_RE = re.compile(
    r"^\s*(NEW|ESCALATED|DE-ESCALATED|UNCHANGED)\s*[|:\-]\s*(.+)$",
    re.IGNORECASE | re.MULTILINE,
)


@dataclass
class ThemeDelta:
    status: str
    theme: str
    line: str


def parse_comparison_sections(text: str) -> list[ThemeDelta]:
    """Extract per-theme delta lines from LLM comparison markdown."""
    deltas: list[ThemeDelta] = []
    for match in _LINE_RE.finditer(text):
        status = match.group(1).upper()
        theme = match.group(2).strip()
        deltas.append(ThemeDelta(status=status, theme=theme, line=match.group(0).strip()))
    return deltas


def rule_based_theme_diff(
    current_summary: str, prior_summary: str
) -> list[ThemeDelta]:
    """
    Lightweight fallback when LLM comparison is unavailable.
    Flags keywords that appear only in current vs prior text.
    """
    def tokens(s: str) -> set[str]:
        return {w.lower() for w in re.findall(r"[A-Za-z]{4,}", s)}

    cur, prev = tokens(current_summary), tokens(prior_summary)
    new_kw = sorted(cur - prev)[:8]
    gone = sorted(prev - cur)[:8]
    deltas: list[ThemeDelta] = []
    for kw in new_kw:
        deltas.append(ThemeDelta("NEW", kw, f"NEW | {kw}"))
    for kw in gone:
        deltas.append(ThemeDelta("DE-ESCALATED", kw, f"DE-ESCALATED | {kw}"))
    if not deltas:
        deltas.append(ThemeDelta("UNCHANGED", "overall", "UNCHANGED | No keyword shift"))
    return deltas
