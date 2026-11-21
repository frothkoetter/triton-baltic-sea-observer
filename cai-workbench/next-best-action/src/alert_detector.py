"""Phase 2: Trino + SITREP alert detection."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from .alert_types import NbaAlert
from .config import Settings
from . import sitrep_bridge
from . import sql_loader
from . import trino_client


def _default_env_payload(extra: dict[str, Any]) -> dict[str, Any]:
    base = {
        "cloud_cover_pct": 65,
        "seastate": 3,
        "night": False,
        "water_depth_m": 22,
        "turbidity_high": False,
    }
    base.update(extra)
    return base


def _float_or(val: Any, default: float) -> float:
    try:
        if val is None:
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


def alert_from_das_cable_row(row: dict[str, Any]) -> NbaAlert:
    lat = _float_or(row.get("segment_lat"), 54.4)
    lon = _float_or(row.get("segment_lon"), 13.5)
    cable = row.get("cable_name") or row.get("cable_asset") or "cable"
    theatre = f"DAS {cable}"
    threat = row.get("predicted_threat_type") or row.get("threat_class")
    payload = _default_env_payload(
        {
            "source": "das_cable_threat",
            "cable_asset": row.get("cable_asset"),
            "cable_name": row.get("cable_name"),
            "threat_class": row.get("threat_class"),
            "predicted_threat_type": row.get("predicted_threat_type"),
            "chainage_m": row.get("chainage_m"),
            "hadoop_file_url": row.get("hadoop_file_url"),
            "segment_index": row.get("segment_index"),
            "model_confidence": row.get("model_confidence"),
            "water_depth_m": 24,
            "turbidity_high": True,
        }
    )
    priority = "FLASH" if str(threat) in ("cui_sabotage_tap", "anchor_drag") else "PRIORITY"
    return NbaAlert(
        flow_uuid=str(uuid.uuid4()),
        alert_type="das_cable_threat",
        priority=priority,
        latitude=lat,
        longitude=lon,
        theatre=theatre,
        created_at=datetime.now(timezone.utc),
        payload=payload,
    )


def alert_from_pipeline_row(row: dict[str, Any]) -> NbaAlert:
    lat = _float_or(row.get("start_lat"), 54.4)
    lon = _float_or(row.get("start_lon"), 13.5)
    theatre = f"Pipeline {row.get('pipeline_name', 'unknown')}"
    payload = _default_env_payload(
        {
            "source": "pipeline_blackout",
            "mmsi": row.get("mmsi"),
            "pipeline_name": row.get("pipeline_name"),
            "medium": row.get("medium"),
            "verdachtsmuster": row.get("verdachtsmuster"),
            "luecke_minuten": row.get("luecke_minuten"),
            "blackout_start": str(row.get("blackout_start")),
            "blackout_ende": str(row.get("blackout_ende")),
            "water_depth_m": 18,
            "turbidity_high": True,
        }
    )
    return NbaAlert(
        flow_uuid=str(uuid.uuid4()),
        alert_type="ais_pipeline_blackout",
        priority="FLASH",
        latitude=lat,
        longitude=lon,
        theatre=theatre,
        created_at=datetime.now(timezone.utc),
        payload=payload,
    )


def alert_from_lagebild_row(row: dict[str, Any]) -> NbaAlert:
    ds = str(row.get("Data_Source") or row.get("data_source") or "lagebild")
    harbour = str(row.get("harbour_name") or "Baltic")
    lat = _float_or(row.get("latitude"), 54.5)
    lon = _float_or(row.get("longitude"), 14.0)
    details = str(row.get("details") or "")
    alert_type = "lagebild_signal"
    priority = "PRIORITY"
    if ds == "GPS_Jammer":
        alert_type = "gps_jammer"
        priority = "FLASH"
    elif ds == "REF":
        alert_type = "reference_vessel"
        priority = "FLASH"
    elif ds == "RUS_Fleet":
        alert_type = "rus_fleet_proximity"
        priority = "PRIORITY"
    elif ds == "DAS_Cable":
        alert_type = "das_cable_threat"
        priority = "FLASH" if "cui_sabotage" in details.lower() or "anchor_drag" in details.lower() else "PRIORITY"
    if "flash" in details.lower():
        alert_type = "lagebild_flash"
        priority = "FLASH"
    payload = _default_env_payload(
        {
            "source": "lagebild",
            "data_source": ds,
            "harbour_name": harbour,
            "lagebild_id": row.get("ID"),
            "details": details,
            "event_ts": str(row.get("event_ts")),
        }
    )
    if alert_type == "gps_jammer":
        payload["turbidity_high"] = False
        payload["cloud_cover_pct"] = 80
    theatre = harbour if harbour else "Baltic Sea"
    return NbaAlert(
        flow_uuid=str(uuid.uuid4()),
        alert_type=alert_type,
        priority=priority,
        latitude=lat,
        longitude=lon,
        theatre=theatre,
        created_at=datetime.now(timezone.utc),
        payload=payload,
    )


def alert_from_sitrep(settings: Settings) -> NbaAlert | None:
    arts = settings.lagebild_artifacts_dir
    hit = sitrep_bridge.latest_escalated_theme(arts)
    if hit is None:
        return None
    theme, line = hit
    payload = sitrep_bridge.sitrep_alert_payload(theme, line)
    return NbaAlert(
        flow_uuid=str(uuid.uuid4()),
        alert_type="sitrep_escalation",
        priority="FLASH",
        latitude=settings.demo_lat,
        longitude=settings.demo_lon,
        theatre=theme[:120],
        created_at=datetime.now(timezone.utc),
        payload=payload,
    )


def fetch_das_cable_threat(settings: Settings) -> NbaAlert | None:
    if not settings.enable_das_cable_threat or not trino_client.trino_configured():
        return None
    q = sql_loader.render_sql(
        "das_cable_threat_top1.sql",
        qualified=sql_loader.qualified_schema(
            settings.trino_catalog, settings.trino_schema
        ),
    )
    try:
        rows = trino_client.fetch_all(q)
    except Exception:
        return None
    if not rows:
        return None
    return alert_from_das_cable_row(rows[0])


def fetch_pipeline_blackout(settings: Settings) -> NbaAlert | None:
    if not settings.enable_pipeline_blackout or not trino_client.trino_configured():
        return None
    q = sql_loader.render_sql(
        "pipeline_blackout_top1.sql",
        qualified=sql_loader.qualified_schema(
            settings.trino_catalog, settings.trino_schema
        ),
    )
    try:
        rows = trino_client.fetch_all(q)
    except Exception:
        return None
    if not rows:
        return None
    return alert_from_pipeline_row(rows[0])


def fetch_lagebild_candidates(settings: Settings) -> list[NbaAlert]:
    if not trino_client.trino_configured():
        return []
    q = sql_loader.render_sql(
        "lagebild_nba_candidates.sql",
        qualified=sql_loader.qualified_schema(
            settings.trino_catalog, settings.trino_schema
        ),
        limit=settings.alert_candidate_limit,
    )
    try:
        rows = trino_client.fetch_all(q)
    except Exception:
        return []
    return [alert_from_lagebild_row(r) for r in rows]


def detect_alerts(settings: Settings) -> list[NbaAlert]:
    """Priority order: pipeline blackout, DAS cable threat, SITREP escalation, lagebild rows."""
    candidates: list[NbaAlert] = []
    pipe = fetch_pipeline_blackout(settings)
    if pipe is not None:
        candidates.append(pipe)
    das = fetch_das_cable_threat(settings)
    if das is not None:
        candidates.append(das)
    sitrep = alert_from_sitrep(settings)
    if sitrep is not None:
        candidates.append(sitrep)
    candidates.extend(fetch_lagebild_candidates(settings))
    return candidates