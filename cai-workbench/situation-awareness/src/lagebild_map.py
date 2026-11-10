"""Static situation map for German Baltic harbours (matplotlib)."""

from __future__ import annotations

import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from .harbour_geo import coords_for_harbour, harbour_points

_SEVERITY_RANK = {"low": 1, "medium": 2, "high": 3, "critical": 4}
_STATUS_PREFIX = re.compile(r"^\s*(NEW|ESCALATED)\s*\|", re.I)


def _max_severity(values: pd.Series) -> str:
    best = "low"
    best_rank = 0
    for v in values.dropna().astype(str):
        key = v.strip().lower()
        rank = _SEVERITY_RANK.get(key, 1)
        if rank > best_rank:
            best_rank = rank
            best = key if key in _SEVERITY_RANK else "medium"
    return best


def _harbour_counts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "harbour_name" not in df.columns:
        return pd.DataFrame(columns=["harbour_name", "count", "severity"])

    grouped = df.groupby("harbour_name", dropna=True)
    rows: list[dict[str, object]] = []
    for harbour, chunk in grouped:
        name = str(harbour).strip()
        if not name or coords_for_harbour(name) is None:
            continue
        severity = (
            _max_severity(chunk["severity"]) if "severity" in chunk.columns else "medium"
        )
        rows.append(
            {"harbour_name": name, "count": len(chunk), "severity": severity}
        )
    return pd.DataFrame(rows)


def _changed_harbours(
    comparison_text: str | None,
    dataset_summary: str,
    counts: pd.DataFrame,
) -> set[str]:
    highlighted: set[str] = set()
    if comparison_text:
        for line in comparison_text.splitlines():
            if not _STATUS_PREFIX.match(line):
                continue
            upper = line.upper()
            for harbour in counts["harbour_name"].tolist():
                if harbour.upper() in upper:
                    highlighted.add(harbour)

    if highlighted:
        return highlighted

    m = re.search(r"busiest harbours\s*[—–-]\s*([^;(]+)", dataset_summary, re.I)
    if m:
        token = m.group(1).strip().split("(")[0].strip()
        if coords_for_harbour(token):
            return {token}

    if not counts.empty:
        top = counts.sort_values("count", ascending=False).iloc[0]
        return {str(top["harbour_name"])}

    return set()


def _severity_color(severity: str) -> tuple[float, float, float, float]:
    key = (severity or "medium").lower()
    palette = {
        "low": (0.35, 0.65, 0.45, 0.85),
        "medium": (0.95, 0.55, 0.15, 0.9),
        "high": (0.85, 0.15, 0.12, 0.95),
        "critical": (0.55, 0.05, 0.08, 0.95),
    }
    return palette.get(key, palette["medium"])


def render_lagebild_map(
    df: pd.DataFrame,
    out_path: Path,
    *,
    comparison_text: str | None = None,
    dataset_summary: str = "",
) -> Path:
    """Write PNG map; circle size = event count, ring = NEW/ESCALATED harbour."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    counts = _harbour_counts(df)
    changed = _changed_harbours(comparison_text, dataset_summary, counts)

    fig, ax = plt.subplots(figsize=(7.2, 5.4), dpi=120)
    ax.set_title("German Baltic — harbour activity (Lagebild)", fontsize=11, pad=10)
    ax.set_xlabel("Longitude °E")
    ax.set_ylabel("Latitude °N")

    # Background extent (German Baltic coast)
    ax.set_xlim(10.0, 14.2)
    ax.set_ylim(53.5, 54.75)
    ax.set_facecolor("#e8f0f8")
    ax.grid(True, linestyle=":", alpha=0.45)

    if counts.empty:
        for hp in harbour_points():
            ax.scatter(hp.longitude, hp.latitude, s=18, c="#888888", alpha=0.5)
            ax.text(hp.longitude + 0.03, hp.latitude + 0.02, hp.name, fontsize=7)
    else:
        max_count = max(int(c) for c in counts["count"]) or 1
        for _, row in counts.iterrows():
            name = str(row["harbour_name"])
            coords = coords_for_harbour(name)
            if not coords:
                continue
            lat, lon = coords
            n = int(row["count"])
            size = 80 + 220 * (n / max_count)
            color = _severity_color(str(row.get("severity", "medium")))
            ax.scatter(lon, lat, s=size, c=[color], edgecolors="none", zorder=3)
            if name in changed:
                ax.scatter(
                    lon,
                    lat,
                    s=size * 1.55,
                    facecolors="none",
                    edgecolors="#c9a227",
                    linewidths=2.4,
                    zorder=4,
                )
                ax.scatter(
                    lon,
                    lat,
                    s=size * 1.72,
                    facecolors="none",
                    edgecolors="#b22222",
                    linewidths=1.0,
                    linestyle="--",
                    zorder=4,
                )
            ax.text(lon + 0.04, lat + 0.025, f"{name} ({n})", fontsize=7.5, zorder=5)

        for hp in harbour_points():
            if hp.name not in set(counts["harbour_name"].astype(str)):
                ax.scatter(hp.longitude, hp.latitude, s=14, c="#aaaaaa", alpha=0.35)
                ax.text(hp.longitude + 0.02, hp.latitude + 0.015, hp.name, fontsize=6, alpha=0.6)

    if changed:
        ax.text(
            10.05,
            53.58,
            "Gold/red ring: NEW or ESCALATED harbour",
            fontsize=7,
            style="italic",
        )

    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    return out_path
