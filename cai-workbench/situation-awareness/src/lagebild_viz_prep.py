"""Column normalization and display rollups for live Trino ``defense.lagebild`` frames."""

from __future__ import annotations

import re
from typing import Any

import numpy as np
import pandas as pd

_HARBOUR_ALIASES = ("harbour_name", "Harbour_name", "Harbour", "harbour", "HARBOUR_NAME")
_SOURCE_ALIASES = ("data_source", "Data_Source", "source", "Source", "DATA_SOURCE")
_SEVERITY_ALIASES = ("severity", "Severity", "SEVERITY")
_DETAILS_ALIASES = ("details", "details_text", "Details", "event_details", "description")
_TIMESTAMP_ALIASES = (
    "event_timestamp",
    "Event_Timestamp",
    "timestamp",
    "Timestamp",
    "event_time",
    "observation_time",
)

_HIGH_SEVERITY_RE = re.compile(
    r"\b(sanctions?|kilo|submarine|critical|frigate|torpedo|jammer|hostile)\b",
    re.I,
)
_LOW_SEVERITY_RE = re.compile(
    r"\b(routine|none|nominal|ferry|scheduled|clear)\b",
    re.I,
)

_FRIENDLY_SOURCE: dict[str, str] = {
    "lidar": "LiDAR",
    "ais": "AIS",
    "marine": "Marine",
    "social_media": "Social Media",
    "social media": "Social Media",
    "twitter": "Social Media",
    "buoy": "Buoy",
    "bojen": "Buoy",
    "gps": "GPS",
    "gps_jammer": "GPS Jammer",
    "gps jammer": "GPS Jammer",
    "radar": "Radar",
    "sar": "SAR",
    "stanag": "STANAG",
    "rus_fleet": "RUS Fleet",
    "rus fleet": "RUS Fleet",
}


def _first_present(df: pd.DataFrame, names: tuple[str, ...]) -> str | None:
    for name in names:
        if name in df.columns:
            return name
    return None


def friendly_source_label(value: object) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "Other"
    text = str(value).strip()
    if not text:
        return "Other"
    key = re.sub(r"[\s\-]+", "_", text).lower()
    key_space = text.lower()
    if key in _FRIENDLY_SOURCE:
        return _FRIENDLY_SOURCE[key]
    if key_space in _FRIENDLY_SOURCE:
        return _FRIENDLY_SOURCE[key_space]
    if key.startswith("social"):
        return "Social Media"
    if "lidar" in key:
        return "LiDAR"
    if key == "ais" or key.startswith("ais_"):
        return "AIS"
    return text.replace("_", " ").title() if "_" in text else text


def infer_severity_from_details(text: object) -> str:
    if text is None or (isinstance(text, float) and np.isnan(text)):
        return "medium"
    blob = str(text)
    if _HIGH_SEVERITY_RE.search(blob):
        return "high"
    if _LOW_SEVERITY_RE.search(blob):
        return "low"
    if re.search(r"\bcritical\b", blob, re.I):
        return "critical"
    return "medium"


def normalize_lagebild_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with standardized ``harbour_name``, ``data_source``, ``severity``, timestamps."""
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy()

    out = df.copy()
    harbour_col = _first_present(out, _HARBOUR_ALIASES)
    if harbour_col and harbour_col != "harbour_name":
        out["harbour_name"] = out[harbour_col]
    elif harbour_col is None:
        out["harbour_name"] = pd.NA

    source_col = _first_present(out, _SOURCE_ALIASES)
    if source_col and source_col != "data_source":
        out["data_source"] = out[source_col]
    elif source_col is None:
        out["data_source"] = pd.NA

    severity_col = _first_present(out, _SEVERITY_ALIASES)
    if severity_col:
        out["severity"] = out[severity_col]
    else:
        details_col = _first_present(out, _DETAILS_ALIASES)
        if details_col:
            out["severity"] = out[details_col].map(infer_severity_from_details)
        else:
            out["severity"] = "medium"

    ts_col = _first_present(out, _TIMESTAMP_ALIASES)
    if ts_col and ts_col != "event_timestamp":
        out["event_timestamp"] = out[ts_col]
    elif ts_col is None and "event_timestamp" not in out.columns:
        out["event_timestamp"] = pd.NaT

    if "data_source" in out.columns:
        out["data_source"] = out["data_source"].map(friendly_source_label)

    if "harbour_name" in out.columns:
        out["harbour_name"] = (
            out["harbour_name"]
            .astype("string")
            .str.strip()
            .replace("", pd.NA)
        )

    return out


def rollup_data_sources(
    series: pd.Series,
    *,
    top_n: int = 6,
    cap_social: bool = True,
    social_min_share: float = 0.05,
    social_min_count: int = 2,
) -> pd.Series:
    """
    Roll long tails into ``Other``; optionally fold small social-media slices into ``Other``.
    """
    if series is None or series.empty:
        return pd.Series(dtype=int)

    labels = series.fillna("Other").astype(str).map(friendly_source_label)
    counts = labels.value_counts()
    total = int(counts.sum())
    if total == 0:
        return counts

    if cap_social and "Social Media" in counts.index:
        sm = int(counts["Social Media"])
        if sm < social_min_count or (sm / total) < social_min_share:
            other = int(counts.get("Other", 0)) + sm
            counts = counts.drop("Social Media")
            counts["Other"] = other

    if len(counts) <= top_n:
        return counts.sort_values(ascending=False)

    head = counts.sort_values(ascending=False).head(top_n)
    tail = counts.drop(head.index, errors="ignore")
    other = int(head.get("Other", 0)) + int(tail.sum())
    head = head.copy()
    if other > 0:
        head["Other"] = other
    elif "Other" in head.index and head["Other"] == 0:
        head = head.drop("Other")
    return head.sort_values(ascending=False)


def _normalize_severity_key(value: object) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "unknown"
    key = str(value).strip().lower()
    if key in ("low", "medium", "high", "critical"):
        return key
    return "unknown"


def visualization_panel_status(
    df: pd.DataFrame,
    comparison_text: str | None = None,
) -> dict[str, bool]:
    """Return which 2x2 chart panels would render non-empty placeholder-free content."""
    from .lagebild_charts import _delta_status_counts

    status: dict[str, bool] = {
        "source_pie": False,
        "harbour_bars": False,
        "severity": False,
        "deltas_or_fallback": False,
    }
    if df is None or df.empty:
        return status

    norm = normalize_lagebild_frame(df)
    rolled = rollup_data_sources(norm["data_source"])
    status["source_pie"] = not rolled.empty

    if "harbour_name" in norm.columns:
        harbour = norm["harbour_name"].dropna()
        status["harbour_bars"] = not harbour.empty

    sev = norm["severity"].map(_normalize_severity_key)
    status["severity"] = bool(sev.notna().any())

    if comparison_text and comparison_text.strip():
        status["deltas_or_fallback"] = bool(_delta_status_counts(comparison_text))
    else:
        status["deltas_or_fallback"] = (
            "data_source" in norm.columns and "harbour_name" in norm.columns
        )
    return status
