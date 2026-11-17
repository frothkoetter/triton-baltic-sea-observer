"""Phase 0 alert ingestion — demo + optional lagebild severity probe."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .config import Settings
from . import trino_client


@dataclass
class NbaAlert:
    flow_uuid: str
    alert_type: str
    priority: str
    latitude: float
    longitude: float
    theatre: str
    created_at: datetime
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "flow_uuid": self.flow_uuid,
            "alert_type": self.alert_type,
            "priority": self.priority,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "theatre": self.theatre,
            "created_at": self.created_at.isoformat(),
            "payload": self.payload,
        }


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


def next_alert(settings: Settings) -> NbaAlert:
    if settings.demo_mode:
        return demo_greifswald_alert(settings)
    flash_n = lagebild_flash_count(settings)
    if flash_n is not None and flash_n > 0:
        alert = demo_greifswald_alert(settings)
        alert.alert_type = "lagebild_flash"
        alert.payload["lagebild_flash_rows"] = flash_n
        return alert
    return demo_greifswald_alert(settings)