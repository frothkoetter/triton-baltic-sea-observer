"""Capture Iceberg snapshot ids per fact table (no branches)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .config import Settings
from . import trino_client


@dataclass(frozen=True)
class SnapshotRef:
    table_name: str
    snapshot_id: int
    committed_at: Any | None


@dataclass
class SnapshotBundle:
    flow_uuid: str
    captured_at: datetime
    snapshots: list[SnapshotRef]

    def to_dict(self) -> dict[str, Any]:
        return {
            "flow_uuid": self.flow_uuid,
            "captured_at": self.captured_at.isoformat(),
            "snapshots": [
                {
                    "table_name": s.table_name,
                    "snapshot_id": s.snapshot_id,
                    "committed_at": str(s.committed_at) if s.committed_at else None,
                }
                for s in self.snapshots
            ],
        }


def _qualified(settings: Settings, table: str) -> str:
    return f"{settings.trino_catalog}.{settings.trino_schema}.{table}"


def latest_snapshot_for_table(settings: Settings, table: str) -> SnapshotRef:
    q = (
        f'SELECT snapshot_id, committed_at FROM '
        f'{settings.trino_catalog}.{settings.trino_schema}."{table}$snapshots" '
        f"ORDER BY committed_at DESC LIMIT 1"
    )
    rows = trino_client.fetch_all(q)
    if not rows:
        raise RuntimeError(f"No snapshots for table {table}")
    row = rows[0]
    return SnapshotRef(
        table_name=table,
        snapshot_id=int(row["snapshot_id"]),
        committed_at=row.get("committed_at"),
    )


def offline_bundle(flow_uuid: str, settings: Settings) -> SnapshotBundle:
    """Deterministic bundle for tests without Trino."""
    now = datetime.now(timezone.utc)
    refs = [
        SnapshotRef(table_name=t, snapshot_id=1, committed_at=now)
        for t in settings.snapshot_tables
    ]
    return SnapshotBundle(flow_uuid=flow_uuid, captured_at=now, snapshots=refs)


def capture_bundle(flow_uuid: str, settings: Settings) -> SnapshotBundle:
    if settings.offline_snapshots or not trino_client.trino_configured():
        return offline_bundle(flow_uuid, settings)

    now = datetime.now(timezone.utc)
    refs: list[SnapshotRef] = []
    for table in settings.snapshot_tables:
        refs.append(latest_snapshot_for_table(settings, table))
    return SnapshotBundle(flow_uuid=flow_uuid, captured_at=now, snapshots=refs)


def time_travel_count(settings: Settings, table: str, snapshot_id: int) -> int:
    q = (
        f"SELECT COUNT(*) AS c FROM {_qualified(settings, table)} "
        f"FOR VERSION AS OF {snapshot_id}"
    )
    rows = trino_client.fetch_all(q)
    return int(rows[0]["c"])