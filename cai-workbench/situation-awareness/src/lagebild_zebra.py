"""Zebra BI-inspired Lagebild analytics (matplotlib Agg)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .lagebild_viz_prep import (
    baltic_harbour_counts,
    normalize_lagebild_frame,
    rollup_radar_sources,
)

# Zebra / NATO palette
AC_NAVY = "#1f4e79"
PY_ORANGE = "#e07a2f"
VARIANCE_UP = "#c62828"  # AC > PY — escalation at harbour
VARIANCE_DOWN = "#2e7d32"  # AC < PY — de-escalation
GRID_GREY = "#d8d8d8"
PY_TEXT = "#7a7a7a"
AC_TEXT = "#1a1a1a"
NATO_HEADER_RGB = (0 / 255, 61 / 255, 122 / 255)
NATO_BAND_RGB = (210 / 255, 218 / 255, 226 / 255)
_TOP_SOURCES = 10
_TOP_HARBOURS = 8

_REGIONAL_TABLE_SOURCES = ("GPS Jammer", "Area violation")


def _source_column(df: pd.DataFrame) -> pd.Series | None:
    if df is None or df.empty:
        return None
    if "data_source" in df.columns:
        return df["data_source"]
    for name in ("Data_Source", "source"):
        if name in df.columns:
            return df[name]
    return None


def _prepare_viz_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    if "viz_region" in df.columns:
        return df.copy()
    return normalize_lagebild_frame(df)


def source_counts_from_df(df: pd.DataFrame) -> dict[str, int]:
    if df is None or df.empty:
        return {}
    work = _prepare_viz_df(df)
    col = _source_column(work)
    if col is None:
        return {}
    rolled = rollup_radar_sources(col, top_n=_TOP_SOURCES, min_spokes=6)
    return {str(k): int(v) for k, v in rolled.items()}


def harbour_counts_from_df(df: pd.DataFrame) -> dict[str, int]:
    return baltic_harbour_counts(df, top_n=12, harbour_only=True)


def radar_spoke_labels(df: pd.DataFrame) -> list[str]:
    """Ordered radar dimension labels (for tests and footnotes)."""
    return list(source_counts_from_df(df).keys())


def regional_feed_counts(df: pd.DataFrame) -> dict[str, int]:
    work = _prepare_viz_df(df)
    col = _source_column(work)
    if col is None:
        return {}
    counts: dict[str, int] = {}
    for label in _REGIONAL_TABLE_SOURCES:
        n = int((col == label).sum())
        if n > 0:
            counts[label] = n
    return counts


def variance_table_row_labels(df: pd.DataFrame) -> list[str]:
    """Harbour rows plus regional pseudo-rows shown in the variance table."""
    harbours = list(
        baltic_harbour_counts(df, top_n=_TOP_HARBOURS, harbour_only=True).keys()
    )
    regional = list(regional_feed_counts(df).keys())
    return harbours + regional


def format_count_k(value: float | int) -> str:
    n = float(value)
    if n >= 1000:
        rounded = n / 1000.0
        if rounded >= 100:
            return f"{int(round(rounded))}K"
        text = f"{rounded:.1f}K"
        return text.replace(".0K", "K")
    return str(int(round(n)))


def _resolve_polar_ax(out_axes_or_figure: Any) -> plt.Axes:
    if isinstance(out_axes_or_figure, plt.Figure):
        return out_axes_or_figure.add_subplot(111, projection="polar")
    return out_axes_or_figure


def source_mix_radar(
    df: pd.DataFrame,
    out_axes_or_figure: Any,
    *,
    prior_counts: dict[str, int] | None = None,
) -> list[str]:
    """Polar radar of threat-feed mix: AC (navy) vs PY (orange) by data source."""
    ax = _resolve_polar_ax(out_axes_or_figure)
    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)

    ac_counts = source_counts_from_df(df)
    py_counts = dict(prior_counts or {})
    if not ac_counts and not py_counts:
        ax.text(0.5, 0.5, "No source data", transform=ax.transAxes, ha="center")
        ax.set_axis_off()
        return []

    ranked = list(ac_counts.keys())
    if not ranked:
        extra = sorted(py_counts, key=lambda k: py_counts.get(k, 0), reverse=True)[
            :_TOP_SOURCES
        ]
        ranked = extra
    if not ranked:
        ax.set_axis_off()
        return []

    angles = np.linspace(0, 2 * np.pi, len(ranked), endpoint=False)
    ac_vals = np.array([ac_counts.get(k, 0) for k in ranked], dtype=float)
    py_vals = np.array([py_counts.get(k, 0) for k in ranked], dtype=float)
    max_val = max(float(np.max(ac_vals)), float(np.max(py_vals)), 1.0)

    ax.set_ylim(0, max_val * 1.2)
    ax.set_xticks(angles)
    ax.set_xticklabels(ranked, fontsize=7.5)
    ax.tick_params(axis="x", pad=12)
    ax.grid(True, color=GRID_GREY, linestyle="-", linewidth=0.8, alpha=0.9)
    ax.set_yticklabels([])
    for ring in ax.yaxis.get_ticklabels():
        ring.set_visible(False)

    ax.plot(angles, ac_vals, color=AC_NAVY, linewidth=2.2, label="AC")
    ax.fill(angles, ac_vals, color=AC_NAVY, alpha=0.18)
    has_prior = bool(py_counts) and any(py_vals > 0)
    if has_prior:
        ax.plot(angles, py_vals, color=PY_ORANGE, linewidth=2.0, label="PY")
        ax.fill(angles, py_vals, color=PY_ORANGE, alpha=0.12)

    for angle, ac_v in zip(angles, ac_vals):
        if ac_v <= 0:
            continue
        ax.annotate(
            format_count_k(ac_v),
            xy=(angle, ac_v),
            xytext=(5, 5),
            textcoords="offset points",
            fontsize=6.5,
            fontweight="bold",
            color=AC_NAVY,
        )

    ax.set_title(
        "Threat feed mix (AC vs prior window)",
        fontsize=10,
        fontweight="bold",
        color=NATO_HEADER_RGB,
        pad=16,
    )
    if has_prior:
        ax.legend(loc="upper right", bbox_to_anchor=(1.25, 1.12), fontsize=7, frameon=False)
    return ranked


def _harbour_prior_counts(
    prior: pd.DataFrame | dict[str, int] | None,
) -> dict[str, int]:
    if prior is None:
        return {}
    if isinstance(prior, dict):
        return {str(k): int(v) for k, v in prior.items()}
    return harbour_counts_from_df(prior)


def _prior_regional_counts(prior: pd.DataFrame | dict[str, int] | None) -> dict[str, int]:
    if prior is None:
        return {}
    if isinstance(prior, dict):
        out = {}
        for label in _REGIONAL_TABLE_SOURCES:
            if label in prior:
                out[label] = int(prior[label])
        return out
    return regional_feed_counts(prior)


def _draw_variance_row(
    ax: plt.Axes,
    *,
    y: float,
    label: str,
    py: int,
    ac: int,
    max_delta: int,
    bar_scale: float,
    italic: bool = False,
) -> None:
    delta = ac - py
    ax.text(
        0.02,
        y,
        label[:22],
        fontsize=8,
        fontstyle="italic" if italic else "normal",
        va="center",
        transform=ax.transAxes,
    )
    ax.text(
        0.35,
        y,
        str(py),
        fontsize=8,
        color=PY_TEXT,
        ha="center",
        va="center",
        transform=ax.transAxes,
    )
    ax.text(
        0.72,
        y,
        str(ac),
        fontsize=8,
        fontweight="bold",
        color=AC_TEXT,
        ha="center",
        va="center",
        transform=ax.transAxes,
    )
    bar_len = (abs(delta) / max_delta) * bar_scale
    bar_color = VARIANCE_UP if delta > 0 else VARIANCE_DOWN if delta < 0 else PY_TEXT
    bar_x = 0.82
    rect = plt.Rectangle(
        (bar_x, y - 0.008),
        bar_len,
        0.016,
        transform=ax.transAxes,
        color=bar_color,
        clip_on=False,
    )
    ax.add_patch(rect)
    sign = f"+{delta}" if delta > 0 else str(delta)
    ax.text(
        bar_x + bar_len + 0.01,
        y,
        sign,
        fontsize=7,
        color=bar_color,
        va="center",
        ha="left",
        transform=ax.transAxes,
    )


def zebra_variance_table(
    df: pd.DataFrame,
    prior: pd.DataFrame | dict[str, int] | None,
    ax: plt.Axes,
) -> None:
    """
    Top harbours: PY vs AC with ΔPY lollipop bars, plus regional GPS Jammer / Area violation rows.
    """
    ax.set_axis_off()
    ac_harbours = baltic_harbour_counts(df, top_n=_TOP_HARBOURS, harbour_only=True)
    regional_ac = regional_feed_counts(df)
    if not ac_harbours and not regional_ac:
        ax.text(0.5, 0.5, "No harbour data", ha="center", va="center", fontsize=9)
        return

    py_all = _harbour_prior_counts(prior)
    py_regional = _prior_regional_counts(prior)

    harbours = list(ac_harbours.keys())
    regional_labels = list(regional_ac.keys())
    rows: list[tuple[str, int, int, bool]] = []
    for h in harbours:
        rows.append((h, py_all.get(h, 0), ac_harbours[h], False))
    for label in regional_labels:
        rows.append((f"{label} (region)", py_regional.get(label, 0), regional_ac[label], True))

    n = len(rows)
    y_step = 0.78 / max(n, 1)
    bar_scale = 0.14

    ax.text(
        0.02,
        0.98,
        "Harbour / feed",
        fontsize=8,
        fontweight="bold",
        color=NATO_HEADER_RGB,
        va="top",
        transform=ax.transAxes,
    )
    ax.text(0.35, 0.98, "PY", fontsize=8, color=PY_TEXT, ha="center", va="top", transform=ax.transAxes)
    ax.text(0.72, 0.98, "AC", fontsize=8, fontweight="bold", color=AC_TEXT, ha="center", va="top", transform=ax.transAxes)
    ax.text(
        0.88,
        0.98,
        "ΔPY",
        fontsize=8,
        fontweight="bold",
        color=NATO_HEADER_RGB,
        ha="center",
        va="top",
        transform=ax.transAxes,
    )

    max_delta = max(abs(ac - py) for _, py, ac, _ in rows)
    max_delta = max(max_delta, 1)

    for idx, (label, py, ac, italic) in enumerate(rows):
        y = 0.9 - (idx + 1) * y_step
        _draw_variance_row(
            ax,
            y=y,
            label=label,
            py=py,
            ac=ac,
            max_delta=max_delta,
            bar_scale=bar_scale,
            italic=italic,
        )

    ax.text(
        0.5,
        0.04,
        "ΔPY: red = event count increase; green = decrease vs prior window",
        fontsize=6.5,
        color=PY_TEXT,
        ha="center",
        va="bottom",
        transform=ax.transAxes,
    )


def render_lagebild_zebra(
    df: pd.DataFrame,
    out_path: Path,
    *,
    prior_df: pd.DataFrame | None = None,
    prior_source_counts: dict[str, int] | None = None,
    prior_harbour_counts: dict[str, int] | None = None,
    comparison_text: str | None = None,
) -> Path:
    """Write composite Zebra-style radar + harbour variance table PNG."""
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if prior_source_counts is None and prior_df is not None:
        prior_source_counts = source_counts_from_df(prior_df)
    if prior_harbour_counts is None and prior_df is not None:
        prior_harbour_counts = harbour_counts_from_df(prior_df)

    fig = plt.figure(figsize=(12.0, 4.6), dpi=120)
    fig.patch.set_facecolor("white")

    band = fig.add_axes([0, 0.88, 1, 0.12])
    band.set_facecolor(NATO_BAND_RGB)
    band.set_xticks([])
    band.set_yticks([])
    for spine in band.spines.values():
        spine.set_visible(False)
    band.text(
        0.5,
        0.55,
        "LAGEBILD — ANALYTICS (ZEBRA STYLE)",
        ha="center",
        va="center",
        fontsize=11,
        fontweight="bold",
        color=NATO_HEADER_RGB,
    )

    ax_radar = fig.add_axes([0.04, 0.12, 0.44, 0.72], projection="polar")
    ax_table = fig.add_axes([0.52, 0.12, 0.46, 0.72])

    spoke_labels: list[str] = []
    if df is None or df.empty:
        ax_radar.text(0.5, 0.5, "No rows", transform=ax_radar.transAxes, ha="center")
        ax_table.text(0.5, 0.5, "No rows", ha="center", va="center")
    else:
        spoke_labels = source_mix_radar(df, ax_radar, prior_counts=prior_source_counts)
        zebra_variance_table(df, prior_harbour_counts or {}, ax_table)

    if spoke_labels:
        feed_note = "Radar feeds: " + ", ".join(spoke_labels)
        fig.text(0.5, 0.03, feed_note, ha="center", fontsize=6.5, color=PY_TEXT)

    if comparison_text and comparison_text.strip():
        fig.text(
            0.5,
            0.07,
            "24h/48h comparison available in SITREP delta section",
            ha="center",
            fontsize=7,
            color=PY_TEXT,
        )

    fig.savefig(out_path, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return out_path
