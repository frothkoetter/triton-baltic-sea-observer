"""Phase 6: pin facts as nba_mat_<flow>_<table> for repeatable simulation reads."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .config import Settings
from .snapshot_bundle import SnapshotBundle
from . import trino_client


def _esc(value: str) -> str:
    return value.replace("'", "''")


def mat_table_name(flow_uuid: str, table_name: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_]", "_", flow_uuid.replace("-", "_"))[:28]
    safe_table = re.sub(r"[^a-zA-Z0-9_]", "_", table_name)
    return f"nba_mat_{slug}_{safe_table}"


@dataclass(frozen=True)
class MaterializeResult:
    source_table: str
    mat_table: str
    snapshot_id: int
    row_limit: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_table": self.source_table,
            "mat_table": self.mat_table,
            "snapshot_id": self.snapshot_id,
            "row_limit": self.row_limit,
        }


def materialize_bundle(
    bundle: SnapshotBundle,
    settings: Settings,
    *,
    row_limit: int | None = None,
) -> list[MaterializeResult]:
    if settings.offline_snapshots or not trino_client.trino_configured():
        return [
            MaterializeResult(
                source_table=ref.table_name,
                mat_table=mat_table_name(bundle.flow_uuid, ref.table_name),
                snapshot_id=ref.snapshot_id,
                row_limit=row_limit or settings.mat_row_limit,
            )
            for ref in bundle.snapshots
        ]

    limit = row_limit or settings.mat_row_limit
    fq = f"{settings.trino_catalog}.{settings.trino_schema}"
    out: list[MaterializeResult] = []

    for ref in bundle.snapshots:
        mat = mat_table_name(bundle.flow_uuid, ref.table_name)
        trino_client.execute_sql(f"DROP TABLE IF EXISTS {fq}.{mat}")
        sql = (
            f"CREATE TABLE {fq}.{mat} AS "
            f"SELECT * FROM {fq}.{ref.table_name} "
            f"FOR VERSION AS OF {ref.snapshot_id} "
            f"LIMIT {int(limit)}"
        )
        trino_client.execute_sql(sql)
        out.append(
            MaterializeResult(
                source_table=ref.table_name,
                mat_table=mat,
                snapshot_id=ref.snapshot_id,
                row_limit=limit,
            )
        )
    return out