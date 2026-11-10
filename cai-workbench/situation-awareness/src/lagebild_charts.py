"""Composite situation charts for Lagebild PDF reports (matplotlib Agg)."""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .compare_reports import DELTA_PREFIXES, parse_comparison_sections, rule_based_theme_diff
from .lagebild_viz_prep import friendly_source_label, rollup_data_sources

# NATO maritime palette (aligned with report_pdf / lagebild_map).
NATO_HEADER_RGB = (0 / 255, 61 / 255, 122 / 255)
NATO_GRID_RGB = (80 / 255, 90 / 255, 100 / 255)
NATO_BAND_RGB = (210 / 255, 218 / 255, 226 / 255)

_SOURCE_COLORS = {
    "ais": "#1f5f8b",
    "lidar": "#3d7c47",
    "marine": "#2e7d4a",
    "social": "#8e44ad",
    "gps": "#6d4c41",
    "buoy": "#6b5b95",
    "radar": "#b86b25",
    "sar": "#5c6bc0",
    "stanag": "#455a64",
    "rus": "#5d4037",
    "other": "#7a848c",
}
_SEVERITY_COLORS = {
    "low": "#3d9e5a",
    "medium": "#e08a1a",
    "high": "#c41e1e",
    "critical": "#5c0a12",
    "unknown": "#9aa3ab",
}
_TOP_HARBOURS = 8


def _source_color(label: str) -> str:
    token = label.lower().replace(" ", "_").split("_")[0]
    if token == "social":
        return _SOURCE_COLORS["social"]
    if token == "gps":
        return _SOURCE_COLORS["gps"]
    if token == "rus":
        return _SOURCE_COLORS["rus"]
    return _SOURCE_COLORS.get(token, _SOURCE_COLORS["other"])


def _normalize_severity(value: object) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "unknown"
    key = str(value).strip().lower()
    return key if key in _SEVERITY_COLORS else "unknown"


def _delta_status_counts(comparison_text: str) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for delta in parse_comparison_sections(comparison_text):
        status = delta.status.upper()
        if status in DELTA_PREFIXES:
            counts[status] += 1
    if not counts:
        for status in ("NEW", "ESCALATED", "DE-ESCALATED", "UNCHANGED"):
            pat = re.compile(
                rf"\b{re.escape(status)}\s*[|:\-]",
                re.IGNORECASE,
            )
            n = len(pat.findall(comparison_text))
            if n:
                counts[status] = n
    if not counts and comparison_text.strip():
        for delta in rule_based_theme_diff(comparison_text, ""):
            counts[delta.status.upper()] += 1
    return dict(counts)


def _style_axis(ax: plt.Axes, *, title: str) -> None:
    ax.set_title(title, fontsize=10, fontweight="bold", color=NATO_HEADER_RGB, pad=8)
    ax.grid(True, axis="both", linestyle=":", color=NATO_GRID_RGB, alpha=0.35)
    ax.tick_params(labelsize=8)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)


def _panel_source_pie(ax: plt.Axes, df: pd.DataFrame) -> None:
    if df.empty or "data_source" not in df.columns:
        ax.text(0.5, 0.5, "No data source column", ha="center", va="center", fontsize=9)
        ax.set_axis_off()
        return
    counts = rollup_data_sources(df["data_source"])
    if counts.empty:
        ax.text(0.5, 0.5, "No events", ha="center", va="center", fontsize=9)
        ax.set_axis_off()
        return
    labels = list(counts.index)
    colors = [_source_color(lbl) for lbl in labels]
    wedges, _texts, autotexts = ax.pie(
        counts.values,
        labels=labels,
        autopct="%1.0f%%",
        startangle=90,
        colors=colors,
        textprops={"fontsize": 7},
        pctdistance=0.72,
    )
    for t in autotexts:
        t.set_fontsize(7)
        t.set_color("white")
        t.set_fontweight("bold")
    _style_axis(ax, title="Share by data source")


def _panel_harbour_bars(ax: plt.Axes, df: pd.DataFrame) -> None:
    if df.empty or "harbour_name" not in df.columns:
        ax.text(0.5, 0.5, "No harbour data", ha="center", va="center", fontsize=9)
        ax.set_axis_off()
        return
    counts = (
        df["harbour_name"]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .value_counts()
        .head(_TOP_HARBOURS)
        .sort_values(ascending=True)
    )
    if counts.empty:
        ax.text(0.5, 0.5, "No harbour events", ha="center", va="center", fontsize=9)
        ax.set_axis_off()
        return
    y = np.arange(len(counts))
    ax.barh(y, counts.values, color=NATO_HEADER_RGB, height=0.65, alpha=0.88)
    ax.set_yticks(y)
    ax.set_yticklabels(counts.index, fontsize=8)
    ax.set_xlabel("Event count", fontsize=8)
    _style_axis(ax, title=f"Events per harbour (top {_TOP_HARBOURS})")


def _panel_severity(ax: plt.Axes, df: pd.DataFrame) -> None:
    order = ["low", "medium", "high", "critical", "unknown"]
    if df.empty:
        ax.text(0.5, 0.5, "No events", ha="center", va="center", fontsize=9)
        ax.set_axis_off()
        return
    if "severity" not in df.columns:
        ax.text(0.5, 0.5, "No severity column", ha="center", va="center", fontsize=9)
        ax.set_axis_off()
        return
    norm = df["severity"].map(_normalize_severity)
    counts = norm.value_counts()
    labels = [s for s in order if s in counts.index and counts[s] > 0]
    if not labels:
        ax.text(0.5, 0.5, "No severity values", ha="center", va="center", fontsize=9)
        ax.set_axis_off()
        return
    vals = [int(counts[s]) for s in labels]
    colors = [_SEVERITY_COLORS[s] for s in labels]
    x = np.arange(len(labels))
    ax.bar(x, vals, color=colors, width=0.62, edgecolor=NATO_GRID_RGB, linewidth=0.6)
    ax.set_xticks(x)
    ax.set_xticklabels([s.upper() for s in labels], fontsize=8)
    ax.set_ylabel("Events", fontsize=8)
    _style_axis(ax, title="Severity distribution")


def _panel_comparison_deltas(ax: plt.Axes, comparison_text: str) -> bool:
    """Render delta bar chart. Returns True when a chart was drawn."""
    counts = _delta_status_counts(comparison_text)
    labels = [k for k in DELTA_PREFIXES if counts.get(k, 0) > 0]
    if not labels:
        return False
    vals = [counts[lbl] for lbl in labels]
    colors = {
        "NEW": "#1f5f8b",
        "ESCALATED": "#c41e1e",
        "DE-ESCALATED": "#3d9e5a",
        "UNCHANGED": "#7a848c",
    }
    x = np.arange(len(labels))
    ax.bar(
        x,
        vals,
        color=[colors.get(lbl, NATO_HEADER_RGB) for lbl in labels],
        width=0.55,
    )
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("Line count", fontsize=8)
    _style_axis(ax, title="24h / 48h delta counts")
    return True


def _panel_source_harbour_grouped(ax: plt.Axes, df: pd.DataFrame) -> None:
    if df.empty or "data_source" not in df.columns or "harbour_name" not in df.columns:
        ax.text(0.5, 0.5, "Insufficient dimensions", ha="center", va="center", fontsize=9)
        ax.set_axis_off()
        return
    work = df[["data_source", "harbour_name"]].dropna()
    if work.empty:
        ax.text(0.5, 0.5, "No cross-tab data", ha="center", va="center", fontsize=9)
        ax.set_axis_off()
        return
    work = work.copy()
    work["_source"] = work["data_source"].map(friendly_source_label)
    work["_harbour"] = work["harbour_name"].astype(str).str.strip()
    pivot = (
        work.groupby(["_harbour", "_source"])
        .size()
        .unstack(fill_value=0)
    )
    harbour_totals = pivot.sum(axis=1).sort_values(ascending=False).head(6)
    pivot = pivot.loc[harbour_totals.index]
    if pivot.empty:
        ax.text(0.5, 0.5, "No grouped counts", ha="center", va="center", fontsize=9)
        ax.set_axis_off()
        return
    sources = list(pivot.columns)
    x = np.arange(len(pivot.index))
    width = 0.8 / max(len(sources), 1)
    for idx, source in enumerate(sources):
        offset = (idx - (len(sources) - 1) / 2) * width
        ax.bar(
            x + offset,
            pivot[source].values,
            width=width * 0.92,
            label=source,
            color=_source_color(source),
        )
    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=25, ha="right", fontsize=7)
    ax.set_ylabel("Events", fontsize=8)
    ax.legend(fontsize=7, loc="upper right", framealpha=0.9)
    _style_axis(ax, title="Source x harbour (top harbours)")


def _panel_events_by_source_stacked(ax: plt.Axes, df: pd.DataFrame) -> None:
    """Stacked bars: top harbours with rolled-up data-source mix (fallback panel 4)."""
    if df.empty or "data_source" not in df.columns or "harbour_name" not in df.columns:
        ax.text(0.5, 0.5, "Insufficient dimensions", ha="center", va="center", fontsize=9)
        ax.set_axis_off()
        return
    work = df[["data_source", "harbour_name"]].dropna().copy()
    if work.empty:
        ax.text(0.5, 0.5, "No cross-tab data", ha="center", va="center", fontsize=9)
        ax.set_axis_off()
        return
    work["_source"] = work["data_source"].map(friendly_source_label)
    work["_harbour"] = work["harbour_name"].astype(str).str.strip()
    harbour_order = (
        work["_harbour"].value_counts().head(6).index.tolist()
    )
    sources = (
        work["_source"].value_counts().head(8).index.tolist()
    )
    if not harbour_order or not sources:
        ax.set_axis_off()
        return
    x = np.arange(len(harbour_order))
    bottom = np.zeros(len(harbour_order), dtype=float)
    for source in sources:
        heights = []
        for harbour in harbour_order:
            n = int(
                ((work["_harbour"] == harbour) & (work["_source"] == source)).sum()
            )
            heights.append(n)
        ax.bar(
            x,
            heights,
            bottom=bottom,
            label=source,
            color=_source_color(source),
            width=0.72,
        )
        bottom = bottom + np.array(heights, dtype=float)
    ax.set_xticks(x)
    ax.set_xticklabels(harbour_order, rotation=25, ha="right", fontsize=7)
    ax.set_ylabel("Events", fontsize=8)
    ax.legend(fontsize=7, loc="upper right", ncol=2, framealpha=0.9)
    _style_axis(ax, title="Events by data source (stacked)")


def render_lagebild_charts(
    df: pd.DataFrame,
    out_path: Path,
    *,
    comparison_text: str | None = None,
) -> Path:
    """Write a single 2x2 composite PNG of Lagebild metrics."""
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fig = plt.figure(figsize=(10.5, 7.2), dpi=120)
    fig.patch.set_facecolor("#f5f7fa")

    band = fig.add_axes([0, 0.93, 1, 0.07])
    band.set_facecolor(NATO_BAND_RGB)
    band.set_xticks([])
    band.set_yticks([])
    for spine in band.spines.values():
        spine.set_visible(False)
    band.text(
        0.5,
        0.55,
        "LAGEBILD — DATA VISUALIZATION (GERMAN BALTIC)",
        ha="center",
        va="center",
        fontsize=12,
        fontweight="bold",
        color=NATO_HEADER_RGB,
    )

    axes = [
        fig.add_subplot(2, 2, 1),
        fig.add_subplot(2, 2, 2),
        fig.add_subplot(2, 2, 3),
        fig.add_subplot(2, 2, 4),
    ]
    plt.subplots_adjust(left=0.08, right=0.96, top=0.9, bottom=0.1, hspace=0.38, wspace=0.32)

    if df is None or df.empty:
        for ax in axes:
            ax.text(0.5, 0.5, "No dataframe rows", ha="center", va="center", fontsize=9)
            ax.set_axis_off()
    else:
        _panel_source_pie(axes[0], df)
        _panel_harbour_bars(axes[1], df)
        _panel_severity(axes[2], df)
        if comparison_text and comparison_text.strip():
            if not _panel_comparison_deltas(axes[3], comparison_text):
                _panel_events_by_source_stacked(axes[3], df)
        else:
            _panel_source_harbour_grouped(axes[3], df)

    fig.savefig(out_path, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return out_path
