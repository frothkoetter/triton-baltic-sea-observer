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
_LAT_ALIASES = ("latitude", "Latitude", "lat", "ship_lat", "vessel_lat")
_LON_ALIASES = ("longitude", "Longitude", "lon", "ship_lon", "vessel_lon")
_AREA_ID_COLS = ("area_id", "Area_ID", "Area_Id")

_AREA_ID_RE = re.compile(
    r"\barea_id\s*[=:]\s*['\"]?([A-Za-z0-9][A-Za-z0-9 _\-]*)",
    re.I,
)
_AREA_NAME_IN_DETAILS_RE = re.compile(
    r"\b(?:area|zone|surveillance)\s*[:\-]\s*([A-Za-z][A-Za-z0-9 _\-]{2,40})",
    re.I,
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
    "ref": "Area violation",
    "area_violation": "Area violation",
    "gps_jammer": "GPS Jammer",
}

_RADAR_PINNED_SOURCES = (
    "GPS Jammer",
    "Area violation",
    "RUS Fleet",
    "LiDAR",
    "AIS",
    "Marine",
    "Buoy",
)


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
    upper = text.upper()
    if upper == "REF":
        return "Area violation"
    if upper == "GPS_JAMMER":
        return "GPS Jammer"
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


def extract_area_id(
    details: object,
    *,
    row: pd.Series | None = None,
) -> str | None:
    """Pull surveillance ``area_id`` from column or ``details`` text (REF / area_violation rows)."""
    if row is not None:
        for col in _AREA_ID_COLS:
            if col in row.index:
                val = row[col]
                if val is not None and not (isinstance(val, float) and np.isnan(val)):
                    token = str(val).strip()
                    if token:
                        return token
    if details is None or (isinstance(details, float) and np.isnan(details)):
        return None
    blob = str(details)
    m = _AREA_ID_RE.search(blob)
    if m:
        return m.group(1).strip()
    m = _AREA_NAME_IN_DETAILS_RE.search(blob)
    if m:
        return m.group(1).strip()
    return None


def viz_region_label(
    harbour: object,
    area_id: str | None,
    latitude: object,
    longitude: object,
) -> str:
    if harbour is not None and not (isinstance(harbour, float) and np.isnan(harbour)):
        if not pd.isna(harbour):
            name = str(harbour).strip()
            if name and name.lower() not in ("none", "<na>", "nan"):
                return name
    if area_id is not None and not pd.isna(area_id):
        token = str(area_id).strip()
        if token:
            return f"Area: {token}"
    if latitude is not None and longitude is not None:
        if not (
            (isinstance(latitude, float) and np.isnan(latitude))
            or (isinstance(longitude, float) and np.isnan(longitude))
        ):
            try:
                lat = float(latitude)
                lon = float(longitude)
                return f"Offshore ({lat:.1f}°N, {lon:.1f}°E)"
            except (TypeError, ValueError):
                pass
    return "Offshore"


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

    lat_col = _first_present(out, _LAT_ALIASES)
    if lat_col and lat_col != "latitude":
        out["latitude"] = out[lat_col]
    elif lat_col is None and "latitude" not in out.columns:
        out["latitude"] = pd.NA

    lon_col = _first_present(out, _LON_ALIASES)
    if lon_col and lon_col != "longitude":
        out["longitude"] = out[lon_col]
    elif lon_col is None and "longitude" not in out.columns:
        out["longitude"] = pd.NA

    details_col = _first_present(out, _DETAILS_ALIASES)

    raw_source = out["data_source"].copy() if "data_source" in out.columns else None
    if "data_source" in out.columns:
        out["data_source"] = out["data_source"].map(friendly_source_label)

    if details_col or any(c in out.columns for c in _AREA_ID_COLS):
        extracted = out.apply(
            lambda row: extract_area_id(
                row[details_col] if details_col and details_col in row.index else None,
                row=row,
            ),
            axis=1,
        )
        if "area_id" not in out.columns:
            out["area_id"] = extracted
        else:
            out["area_id"] = out["area_id"].fillna(extracted)
    elif "area_id" not in out.columns:
        out["area_id"] = pd.NA

    if "harbour_name" in out.columns:
        out["harbour_name"] = (
            out["harbour_name"]
            .astype("string")
            .str.strip()
            .replace("", pd.NA)
        )

    out["viz_region"] = out.apply(
        lambda row: viz_region_label(
            row.get("harbour_name"),
            row.get("area_id") if "area_id" in row.index else None,
            row.get("latitude"),
            row.get("longitude"),
        ),
        axis=1,
    )

    return out


def baltic_harbour_counts(
    df: pd.DataFrame,
    *,
    top_n: int = 12,
    harbour_only: bool = True,
) -> dict[str, int]:
    """Top harbour names from data (all Baltic rows, not geo-clipped to Germany)."""
    if df is None or df.empty:
        return {}
    work = normalize_lagebild_frame(df) if "viz_region" not in df.columns else df.copy()
    col = "harbour_name" if harbour_only else "viz_region"
    if col not in work.columns:
        return {}
    series = work[col].dropna().astype(str).str.strip()
    series = series[series != ""]
    if series.empty:
        return {}
    return {str(k): int(v) for k, v in series.value_counts().head(top_n).items()}


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


def rollup_radar_sources(
    series: pd.Series,
    *,
    top_n: int = 10,
    min_spokes: int = 6,
) -> pd.Series:
    """
    Radar-specific rollup: keep every feed with events, pin GPS Jammer / Area violation,
    and use at least ``min_spokes`` dimensions when the data supports it.
    """
    if series is None or series.empty:
        return pd.Series(dtype=int)

    labels = series.fillna("Other").astype(str).map(friendly_source_label)
    counts = labels.value_counts()
    positive = counts[counts > 0]
    if positive.empty:
        return positive

    order: list[str] = []
    for name in positive.sort_values(ascending=False).index:
        if name not in order:
            order.append(str(name))

    for pin in _RADAR_PINNED_SOURCES:
        if pin in positive.index and pin not in order:
            order.append(pin)

    target = min(top_n, max(min_spokes, len(positive)) if len(positive) >= min_spokes else len(positive))
    while len(order) < target:
        added = False
        for name in positive.sort_values(ascending=False).index:
            if str(name) not in order:
                order.append(str(name))
                added = True
                break
        if not added:
            break

    order = order[:top_n]
    out = positive.reindex(order).fillna(0).astype(int)
    return out.sort_values(ascending=False)


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
