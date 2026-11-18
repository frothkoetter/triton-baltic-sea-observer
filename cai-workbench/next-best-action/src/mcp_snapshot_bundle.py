"""Phase 8a: capture Iceberg snapshot ids via iceberg-mcp-server-trino tools."""

from __future__ import annotations

import json
import os
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .config import Settings
from .snapshot_bundle import SnapshotBundle, SnapshotRef, latest_snapshot_for_table

_CDW = Path(__file__).resolve().parents[3] / "cdw-analyse"
if _CDW.is_dir() and str(_CDW) not in sys.path:
    sys.path.insert(0, str(_CDW))

CAPTURE_SOURCE_MCP = "iceberg-mcp-server-trino"
CAPTURE_SOURCE_JDBC_FALLBACK = "jdbc-fallback"


def _mcp_import_error() -> str | None:
    try:
        import iceberg_mcp_server_trino.tools.iceberg_semantics  # noqa: F401
        import iceberg_mcp_server_trino.tools.trino_tools  # noqa: F401
    except ImportError as exc:
        return str(exc)
    return None


@contextmanager
def _trino_env_for_mcp(settings: Settings) -> Iterator[None]:
    """Align MCP Trino env with NBA Settings (catalog/schema + JDBC credentials)."""
    from run_trino_ddl import apply_jdbc_url_env  # noqa: WPS433

    previous = {
        "TRINO_CATALOG": os.environ.get("TRINO_CATALOG"),
        "TRINO_SCHEMA": os.environ.get("TRINO_SCHEMA"),
    }
    apply_jdbc_url_env()
    os.environ["TRINO_CATALOG"] = settings.trino_catalog
    os.environ["TRINO_SCHEMA"] = settings.trino_schema
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _parse_list_snapshots(raw: str) -> dict[str, Any]:
    data = json.loads(raw)
    if "error" in data:
        raise RuntimeError(data["error"])
    snapshots = data.get("snapshots") or []
    if not snapshots:
        raise RuntimeError(data.get("table", "table") + ": no snapshots")
    row = snapshots[0]
    return row


def _short_table_name(qualified: str, table: str) -> str:
    parts = qualified.split(".")
    if parts:
        return parts[-1]
    return table


def snapshot_ref_from_mcp_row(table: str, row: dict[str, Any], qualified: str | None) -> SnapshotRef:
    return SnapshotRef(
        table_name=_short_table_name(qualified or "", table),
        snapshot_id=int(row["snapshot_id"]),
        committed_at=row.get("committed_at"),
    )


def list_latest_snapshot_via_mcp(table: str, settings: Settings) -> tuple[SnapshotRef, dict[str, Any]]:
    from iceberg_mcp_server_trino.tools import iceberg_semantics, trino_tools

    with _trino_env_for_mcp(settings):
        raw = iceberg_semantics.list_snapshots(table, limit=1)
        row = _parse_list_snapshots(raw)
        payload = json.loads(raw)
        qualified = payload.get("table")
        ref = snapshot_ref_from_mcp_row(table, row, qualified)
        health_raw = trino_tools.get_table_health(table)
        health = json.loads(health_raw)
    return ref, {"list_snapshots": payload, "table_health": health}


def build_mcp_audit(
    *,
    flow_uuid: str,
    captured_at: datetime,
    capture_source: str,
    tables: list[dict[str, Any]],
    package_error: str | None = None,
) -> dict[str, Any]:
    audit: dict[str, Any] = {
        "flow_uuid": flow_uuid,
        "captured_at": captured_at.isoformat(),
        "capture_source": capture_source,
        "tables": tables,
    }
    if package_error:
        audit["package_import_error"] = package_error
    return audit


def capture_bundle_via_mcp(flow_uuid: str, settings: Settings) -> tuple[SnapshotBundle, dict[str, Any]]:
    """Try MCP list_snapshots per table; fall back to JDBC per table on failure."""
    now = datetime.now(timezone.utc)
    package_error = _mcp_import_error()
    refs: list[SnapshotRef] = []
    table_audits: list[dict[str, Any]] = []
    any_mcp = False
    any_fallback = False

    if package_error is None:
        for table in settings.snapshot_tables:
            try:
                ref, detail = list_latest_snapshot_via_mcp(table, settings)
                refs.append(ref)
                table_audits.append(
                    {
                        "table_name": ref.table_name,
                        "capture_source": CAPTURE_SOURCE_MCP,
                        "snapshot_id": ref.snapshot_id,
                        "detail": detail,
                    }
                )
                any_mcp = True
            except Exception as exc:  # noqa: BLE001 — per-table fallback
                ref = latest_snapshot_for_table(settings, table)
                refs.append(ref)
                any_fallback = True
                table_audits.append(
                    {
                        "table_name": ref.table_name,
                        "capture_source": CAPTURE_SOURCE_JDBC_FALLBACK,
                        "snapshot_id": ref.snapshot_id,
                        "error": str(exc),
                    }
                )
    else:
        for table in settings.snapshot_tables:
            ref = latest_snapshot_for_table(settings, table)
            refs.append(ref)
            any_fallback = True
            table_audits.append(
                {
                    "table_name": ref.table_name,
                    "capture_source": CAPTURE_SOURCE_JDBC_FALLBACK,
                    "snapshot_id": ref.snapshot_id,
                }
            )

    if any_mcp and any_fallback:
        capture_source = f"{CAPTURE_SOURCE_MCP}+{CAPTURE_SOURCE_JDBC_FALLBACK}"
    elif any_mcp:
        capture_source = CAPTURE_SOURCE_MCP
    else:
        capture_source = CAPTURE_SOURCE_JDBC_FALLBACK

    bundle = SnapshotBundle(flow_uuid=flow_uuid, captured_at=now, snapshots=refs)
    audit = build_mcp_audit(
        flow_uuid=flow_uuid,
        captured_at=now,
        capture_source=capture_source,
        tables=table_audits,
        package_error=package_error,
    )
    return bundle, audit