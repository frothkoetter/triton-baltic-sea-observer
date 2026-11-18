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
    materialize_facts: bool = field(
        default_factory=lambda: _env_bool("NBA_MATERIALIZE", False)
    )
    mat_row_limit: int = field(
        default_factory=lambda: int(os.environ.get("NBA_MAT_ROW_LIMIT", "25000"))
    )
    demo_mode: bool = field(
        default_factory=lambda: _env_bool("NBA_DEMO_GREIFSWALD", False)
    )
    offline_snapshots: bool = field(
        default_factory=lambda: _env_bool("NBA_OFFLINE", False)
    )
    use_iceberg_mcp: bool = field(
        default_factory=lambda: _env_bool("NBA_USE_ICEBERG_MCP", False)
    )

    # Greifswald / Adlergrund reference
    demo_lat: float = field(default_factory=lambda: float(os.environ.get("NBA_DEMO_LAT", "54.55")))
    demo_lon: float = field(default_factory=lambda: float(os.environ.get("NBA_DEMO_LON", "14.10")))
    sim_ticks: int = field(
        default_factory=lambda: int(os.environ.get("NBA_SIM_TICKS", "6"))
    )
    phase: int = field(default_factory=lambda: int(os.environ.get("NBA_PHASE", "1")))

    exit_if_quiet: bool = field(
        default_factory=lambda: _env_bool("NBA_EXIT_IF_QUIET", False)
    )
    dedupe_hours: float = field(
        default_factory=lambda: float(os.environ.get("NBA_DEDUPE_HOURS", "6"))
    )
    alert_candidate_limit: int = field(
        default_factory=lambda: int(os.environ.get("NBA_ALERT_LIMIT", "5"))
    )
    enable_pipeline_blackout: bool = field(
        default_factory=lambda: _env_bool("NBA_PIPELINE_BLACKOUT", True)
    )
    lagebild_artifacts_dir: Path | None = field(default=None)
    skip_llm: bool = field(
        default_factory=lambda: _env_bool("NBA_SKIP_LLM", False)
    )

    def __post_init__(self) -> None:
        raw = os.environ.get("NBA_LAGEBILD_ARTIFACTS_DIR")
        if raw:
            self.lagebild_artifacts_dir = Path(raw)