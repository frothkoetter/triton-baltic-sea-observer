"""Phase 8.4 CDP hooks: Ranger / Data Marketplace MCP context (stub artifact)."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA = "nba-access-context/1"


def build_access_context(
    *,
    flow_uuid: str,
    data_class: str | None = None,
) -> dict[str, Any]:
    tier = (data_class or os.environ.get("NBA_DATA_CLASS", "defense")).strip().lower()
    return {
        "schema": SCHEMA,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "flow_uuid": flow_uuid,
        "data_class": tier,
        "mcp_hooks": {
            "ranger_mcp_server": {
                "purpose": "Column/row policies — OSINT vs national split",
                "env_hint": "RANGER_REST_URL, RANGER_USER (see docs/CDP_PHASE84.md)",
            },
            "data_marketplace_mcp_server": {
                "purpose": "Business glossary for COA / theatre labels",
                "env_hint": "MARKETPLACE_API_URL (see docs/CDP_PHASE84.md)",
            },
        },
        "triton_defaults": {
            "trino_schema": os.environ.get("NBA_TRINO_SCHEMA", "defense"),
            "catalog": os.environ.get("NBA_TRINO_CATALOG", "iceberg"),
        },
    }


def write_access_context(artifact_dir: Path, flow_uuid: str, data_class: str | None = None) -> Path:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    path = artifact_dir / "access_context.json"
    path.write_text(
        json.dumps(
            build_access_context(flow_uuid=flow_uuid, data_class=data_class),
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path