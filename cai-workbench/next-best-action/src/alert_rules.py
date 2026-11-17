"""Alert ingestion: demo, Phase 2 detection, dedupe."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from .alert_detector import detect_alerts
from .alert_types import NbaAlert
from .config import Settings
from . import trino_client


class NoAlertFound(Exception):
    """Raised when event-driven job finds no actionable alert."""


def demo_greifswald_alert(settings: Settings) -> NbaAlert:
    return NbaAlert(
        flow_uuid=str(uuid.uuid4()),
        alert_type="demo_greifswald",
        priority="FLASH",
        latitude=settings.demo_lat,
        longitude=settings.demo_lon,
        theatre="Greifswald / Adlergrund",
        created_at=datetime.now(timezone.utc),
        payload={
            "triggers": ["ais_dropout", "acoustic_transient"],
            "cloud_cover_pct": 82,
            "seastate": 4,
            "night": True,
            "water_depth_m": 18,
            "turbidity_high": True,
        },
    )


def lagebild_flash_count(settings: Settings) -> int | None:
    """Count FLASH-level rows on live lagebild view; None if Trino unavailable."""
    if not trino_client.trino_configured():
        return None
    q = (
        f"SELECT COUNT(*) AS c FROM {settings.trino_catalog}.{settings.trino_schema}.lagebild "
        "WHERE LOWER(COALESCE(CAST(details AS varchar), '')) LIKE '%flash%'"
    )
    try:
        rows = trino_client.fetch_all(q)
        return int(rows[0]["c"])
    except Exception:
        return None


def _esc(value: str) -> str:
    return value.replace("'", "''")


def recent_incident_exists(alert: NbaAlert, settings: Settings) -> bool:
    """Skip duplicates when nba_incidents already has a nearby row."""
    hours = settings.dedupe_hours
    if hours <= 0 or not trino_client.trino_configured():
        return False
    lid = alert.payload.get("lagebild_id")
    base = (
        f"SELECT flow_uuid FROM {settings.trino_catalog}.{settings.trino_schema}.nba_incidents "
        f"WHERE alert_type = '{_esc(alert.alert_type)}' "
        f"AND created_at >= current_timestamp - interval '{int(hours)}' hour "
    )
    if alert.alert_type == "gps_jammer" and lid:
        q = (
            base
            + f" AND json_extract_scalar(alert_payload, '$.lagebild_id') = '{_esc(str(lid))}' "
            "LIMIT 1"
        )
    else:
        q = (
            base
            + f"AND ABS(latitude - {alert.latitude}) < 0.05 "
            f"AND ABS(longitude - {alert.longitude}) < 0.05 "
            "LIMIT 1"
        )
    try:
        rows = trino_client.fetch_all(q)
        return bool(rows)
    except Exception:
        return False


def _pick_alert(candidates: list[NbaAlert], settings: Settings) -> NbaAlert | None:
    for alert in candidates:
        if not recent_incident_exists(alert, settings):
            return alert
    return None


def next_alert(settings: Settings) -> NbaAlert:
    if settings.demo_mode:
        return demo_greifswald_alert(settings)

    candidates = detect_alerts(settings)
    chosen = _pick_alert(candidates, settings)
    if chosen is not None:
        return chosen

    # Legacy fallback when Trino is up but spatial tables are missing
    flash_n = lagebild_flash_count(settings)
    if flash_n is not None and flash_n > 0:
        alert = demo_greifswald_alert(settings)
        alert.alert_type = "lagebild_flash"
        alert.flow_uuid = str(uuid.uuid4())
        alert.payload = dict(alert.payload)
        alert.payload["lagebild_flash_rows"] = flash_n
        alert.payload["source"] = "lagebild_flash_fallback"
        if not recent_incident_exists(alert, settings):
            return alert

    raise NoAlertFound("no actionable NBA alert (dedupe or idle)")