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

from .compare_reports import DELTA_PREFIXES, parse_comparison_sections

# NATO maritime palette (aligned with report_pdf / lagebild_map).
NATO_HEADER_RGB = (0 / 255, 61 / 255, 122 / 255)
NATO_GRID_RGB = (80 / 255, 90 / 255, 100 / 255)
NATO_BAND_RGB = (210 / 255, 218 / 255, 226 / 255)

_SOURCE_COLORS = {
    "ais": "#1f5f8b",
    "lidar": "#3d7c47",
    "buoy": "#6b5b95",
    "radar": "#b86b25",
    "sar": "#5c6bc0",
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


def _source_column(df: pd.DataFrame) -> str | None:
    for name in ("data_source", "Data_Source"):
        if name in df.columns:
            return name
    return None


def _normalize_source(value: object) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "Other"
    text = str(value).strip()
    return text if text else "Other"


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
                rf"^\s*{re.escape(status)}\s*[|:\-]",
                re.IGNORECASE | re.MULTILINE,
            )
            counts[status] = len(pat.findall(comparison_text))
    return dict(counts)


def _style_axis(ax: plt.Axes, *, title: str) -> None:
    ax.set_title(title, fontsize=10, fontweight="bold", color=NATO_HEADER_RGB, pad=8)
    ax.grid(True, axis="both", linestyle=":", color=NATO_GRID_RGB, alpha=0.35)
    ax.tick_params(labelsize=8)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)


def _panel_source_pie(ax: plt.Axes, df: pd.DataFrame) -> None:
    col = _source_column(df)
    if not col or df.empty:
        ax.text(0.5, 0.5, "No data source column", ha="center", va="center", fontsize=9)
        ax.set_axis_off()
        return
    counts = (
        df[col].map(_normalize_source).value_counts().sort_values(ascending=False)
    )
    if counts.empty:
        ax.text(0.5, 0.5, "No events", ha="center", va="center", fontsize=9)
        ax.set_axis_off()
        return
    labels = list(counts.index)
    colors = [
        _SOURCE_COLORS.get(lbl.lower().split()[0], _SOURCE_COLORS["other"])
        for lbl in labels
    ]
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
    if df.empty or "severity" not in df.columns:
        ax.text(0.5, 0.5, "No severity column", ha="center", va="center", fontsize=9)
        ax.set_axis_off()
        return
    norm = df["severity"].map(_normalize_severity)
    counts = norm.value_counts()
    labels = [s for s in order if s in counts.index]
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


def _panel_comparison_deltas(ax: plt.Axes, comparison_text: str) -> None:
    counts = _delta_status_counts(comparison_text)
    labels = [k for k in DELTA_PREFIXES if counts.get(k, 0) > 0]
    if not labels:
        ax.text(
            0.5,
            0.5,
            "No delta lines parsed",
            ha="center",
            va="center",
            fontsize=9,
        )
        ax.set_axis_off()
        return
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


def _panel_source_harbour_grouped(ax: plt.Axes, df: pd.DataFrame) -> None:
    col = _source_column(df)
    if df.empty or not col or "harbour_name" not in df.columns:
        ax.text(0.5, 0.5, "Insufficient dimensions", ha="center", va="center", fontsize=9)
        ax.set_axis_off()
        return
    work = df[[col, "harbour_name"]].dropna()
    if work.empty:
        ax.text(0.5, 0.5, "No cross-tab data", ha="center", va="center", fontsize=9)
        ax.set_axis_off()
        return
    work = work.copy()
    work["_source"] = work[col].map(_normalize_source)
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
        color = _SOURCE_COLORS.get(source.lower().split()[0], _SOURCE_COLORS["other"])
        ax.bar(
            x + offset,
            pivot[source].values,
            width=width * 0.92,
            label=source,
            color=color,
        )
    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=25, ha="right", fontsize=7)
    ax.set_ylabel("Events", fontsize=8)
    ax.legend(fontsize=7, loc="upper right", framealpha=0.9)
    _style_axis(ax, title="Source x harbour (top harbours)")


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
            _panel_comparison_deltas(axes[3], comparison_text)
        else:
            _panel_source_harbour_grouped(axes[3], df)

    fig.savefig(out_path, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return out_path
