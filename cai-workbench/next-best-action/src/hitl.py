"""Phase 4: operator approval written back to Trino."""

from __future__ import annotations

import os
from datetime import datetime, timezone

from .config import Settings
from . import trino_client


def _esc(value: str) -> str:
    return value.replace("'", "''")


def sitrep_link_for_alert(alert_payload: dict, flow_uuid: str, priority: str) -> str | None:
    if alert_payload.get("source") == "sitrep_escalation":
        line = alert_payload.get("comparison_line") or alert_payload.get("theme")
        prec = os.environ.get("LAGEBILD_PRECEDENCE_ESCALATED", "FLASH / IMMEDIATE")
        return f"{prec} | {line or 'ESCALATED'}"
    if priority == "FLASH":
        prec = os.environ.get("LAGEBILD_PRECEDENCE_ESCALATED", "FLASH / IMMEDIATE")
        return f"{prec} | NBA flow_uuid={flow_uuid}"
    return None


def approve_incident(
    flow_uuid: str,
    coa_id: str,
    settings: Settings | None = None,
    *,
    approved_by: str = "cli-operator",
) -> None:
    settings = settings or Settings()
    if coa_id.upper() not in ("A", "B", "C", "D"):
        raise ValueError("coa_id must be A, B, C, or D")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    sql = (
        f"UPDATE {settings.trino_catalog}.{settings.trino_schema}.nba_incidents "
        f"SET approved_coa_id = '{_esc(coa_id.upper())}', "
        f"approved_at = TIMESTAMP '{now}', "
        f"approved_by = '{_esc(approved_by)}', "
        f"status = 'APPROVED', "
        f"updated_at = TIMESTAMP '{now}' "
        f"WHERE flow_uuid = '{_esc(flow_uuid)}'"
    )
    trino_client.execute_sql(sql)