"""Zebra BI-inspired Lagebild analytics (matplotlib Agg)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

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
_TOP_SOURCES = 8
_TOP_HARBOURS = 6


def _source_column(df: pd.DataFrame) -> str | None:
    for name in ("data_source", "Data_Source"):
        if name in df.columns:
            return name
    return None


def source_counts_from_df(df: pd.DataFrame) -> dict[str, int]:
    col = _source_column(df)
    if col is None or df.empty:
        return {}
    counts = df[col].fillna("Unknown").astype(str).value_counts()
    return {str(k): int(v) for k, v in counts.items()}


def harbour_counts_from_df(df: pd.DataFrame) -> dict[str, int]:
    if df is None or df.empty or "harbour_name" not in df.columns:
        return {}
    counts = df["harbour_name"].fillna("Unknown").astype(str).value_counts()
    return {str(k): int(v) for k, v in counts.items()}


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
) -> None:
    """Polar radar of threat-feed mix: AC (navy) vs PY (orange) by data source."""
    ax = _resolve_polar_ax(out_axes_or_figure)
    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)

    ac_counts = source_counts_from_df(df)
    py_counts = dict(prior_counts or {})
    if not ac_counts and not py_counts:
        ax.text(0.5, 0.5, "No source data", transform=ax.transAxes, ha="center")
        ax.set_axis_off()
        return

    ranked = sorted(
        set(ac_counts) | set(py_counts),
        key=lambda k: ac_counts.get(k, 0),
        reverse=True,
    )[:_TOP_SOURCES]
    if not ranked:
        ax.set_axis_off()
        return

    angles = np.linspace(0, 2 * np.pi, len(ranked), endpoint=False)
    ac_vals = np.array([ac_counts.get(k, 0) for k in ranked], dtype=float)
    py_vals = np.array([py_counts.get(k, 0) for k in ranked], dtype=float)
    max_val = max(float(np.max(ac_vals)), float(np.max(py_vals)), 1.0)

    ax.set_ylim(0, max_val * 1.15)
    ax.set_xticks(angles)
    ax.set_xticklabels(ranked, fontsize=8)
    ax.tick_params(axis="x", pad=10)
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

    label_idx = int(np.argmax(ac_vals))
    label_angle = angles[label_idx]
    ax.annotate(
        format_count_k(ac_vals[label_idx]),
        xy=(label_angle, ac_vals[label_idx]),
        xytext=(6, 6),
        textcoords="offset points",
        fontsize=8,
        fontweight="bold",
        color=AC_NAVY,
    )
    if has_prior and py_vals[label_idx] > 0:
        ax.annotate(
            format_count_k(py_vals[label_idx]),
            xy=(label_angle, py_vals[label_idx]),
            xytext=(6, -10),
            textcoords="offset points",
            fontsize=7,
            color=PY_ORANGE,
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


def _harbour_prior_counts(
    prior: pd.DataFrame | dict[str, int] | None,
) -> dict[str, int]:
    if prior is None:
        return {}
    if isinstance(prior, dict):
        return {str(k): int(v) for k, v in prior.items()}
    return harbour_counts_from_df(prior)


def zebra_variance_table(
    df: pd.DataFrame,
    prior: pd.DataFrame | dict[str, int] | None,
    ax: plt.Axes,
) -> None:
    """
    Top harbours: PY vs AC with ΔPY lollipop bars.

    Escalation logic (event counts per harbour): red when AC > PY, green when AC < PY.
    """
    ax.set_axis_off()
    ac_counts = harbour_counts_from_df(df)
    if not ac_counts:
        ax.text(0.5, 0.5, "No harbour data", ha="center", va="center", fontsize=9)
        return

    py_counts = _harbour_prior_counts(prior)
    harbours = sorted(ac_counts, key=lambda h: ac_counts[h], reverse=True)[
        :_TOP_HARBOURS
    ]
    if not harbours:
        return

    n = len(harbours)
    y_step = 1.0 / (n + 1)
    bar_scale = 0.14

    ax.text(
        0.02,
        0.98,
        "Harbour",
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

    max_delta = max(
        abs(ac_counts.get(h, 0) - py_counts.get(h, 0)) for h in harbours
    )
    max_delta = max(max_delta, 1)

    for idx, harbour in enumerate(harbours):
        y = 0.88 - (idx + 1) * y_step
        ac = ac_counts.get(harbour, 0)
        py = py_counts.get(harbour, 0)
        delta = ac - py

        ax.text(0.02, y, harbour[:18], fontsize=8, va="center", transform=ax.transAxes)
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

    ax.text(
        0.5,
        0.04,
        "ΔPY: red = event count increase (escalation); green = decrease vs prior window",
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

    fig = plt.figure(figsize=(10.5, 4.2), dpi=120)
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

    ax_radar = fig.add_axes([0.05, 0.08, 0.42, 0.76], projection="polar")
    ax_table = fig.add_axes([0.52, 0.08, 0.46, 0.76])

    if df is None or df.empty:
        ax_radar.text(0.5, 0.5, "No rows", transform=ax_radar.transAxes, ha="center")
        ax_table.text(0.5, 0.5, "No rows", ha="center", va="center")
    else:
        source_mix_radar(df, ax_radar, prior_counts=prior_source_counts)
        zebra_variance_table(df, prior_harbour_counts or {}, ax_table)

    if comparison_text and comparison_text.strip():
        fig.text(
            0.5,
            0.02,
            "24h/48h comparison available in SITREP delta section",
            ha="center",
            fontsize=7,
            color=PY_TEXT,
        )

    fig.savefig(out_path, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return out_path
