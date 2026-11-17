"""Optional link from UC1 situation-awareness ESCALATED comparison to NBA alert."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

_ESCALATED_LINE = re.compile(
    r"^\s*ESCALATED\s*[|:\-]\s*(.+)$",
    re.IGNORECASE | re.MULTILINE,
)


def _default_artifacts_dir() -> Path:
    here = Path(__file__).resolve().parent.parent
    return (here.parent / "situation-awareness" / "artifacts" / "reports").resolve()


def latest_escalated_theme(artifacts_dir: Path | None) -> tuple[str, str] | None:
    """Return (theme_line, comparison_excerpt) when latest report comparison has ESCALATED."""
    base = artifacts_dir or _default_artifacts_dir()
    index_path = base / "index.json"
    if not index_path.is_file():
        return None
    records = json.loads(index_path.read_text(encoding="utf-8"))
    records.sort(key=lambda r: r.get("timestamp", ""), reverse=True)
    for rec in records:
        txt_name = rec.get("text")
        if not txt_name:
            continue
        txt_path = base / txt_name
        if not txt_path.is_file():
            continue
        body = txt_path.read_text(encoding="utf-8")
        if "ESCALATED" not in body.upper():
            continue
        for line in body.splitlines():
            m = _ESCALATED_LINE.match(line.strip())
            if m:
                theme = m.group(1).strip()
                return theme, line.strip()
        return "situation-awareness", "ESCALATED (comparison present)"
    return None


def sitrep_alert_payload(theme: str, line: str, report_ts: str | None = None) -> dict:
    return {
        "source": "sitrep_escalation",
        "comparison_line": line,
        "theme": theme,
        "report_timestamp": report_ts or datetime.now(timezone.utc).isoformat(),
        "cloud_cover_pct": 60,
        "seastate": 3,
        "night": False,
        "water_depth_m": 25,
        "turbidity_high": False,
    }