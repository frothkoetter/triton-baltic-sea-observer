"""Shared alert datatypes (no Trino imports)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


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

    @property
    def dedupe_key(self) -> str:
        return f"{self.alert_type}:{self.theatre[:80]}"