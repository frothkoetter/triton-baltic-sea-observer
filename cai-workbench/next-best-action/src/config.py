"""Environment configuration for NBA Phase 0."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _split_csv(name: str, default: str) -> list[str]:
    raw = os.environ.get(name, default)
    return [p.strip() for p in raw.split(",") if p.strip()]


@dataclass
class Settings:
    trino_catalog: str = field(
        default_factory=lambda: os.environ.get("NBA_TRINO_CATALOG", "iceberg")
    )
    trino_schema: str = field(
        default_factory=lambda: os.environ.get("NBA_TRINO_SCHEMA", "defense")
    )
    snapshot_tables: list[str] = field(
        default_factory=lambda: _split_csv(
            "NBA_SNAPSHOT_TABLES", "ships,ais_events_ice"
        )
    )
    artifacts_dir: Path = field(default_factory=lambda: Path("artifacts/nba"))
    persist_trino: bool = field(
        default_factory=lambda: _env_bool("NBA_PERSIST_TRINO", False)
    )
    demo_mode: bool = field(
        default_factory=lambda: _env_bool("NBA_DEMO_GREIFSWALD", False)
    )
    offline_snapshots: bool = field(
        default_factory=lambda: _env_bool("NBA_OFFLINE", False)
    )

    # Greifswald / Adlergrund reference
    demo_lat: float = field(default_factory=lambda: float(os.environ.get("NBA_DEMO_LAT", "54.55")))
    demo_lon: float = field(default_factory=lambda: float(os.environ.get("NBA_DEMO_LON", "14.10")))