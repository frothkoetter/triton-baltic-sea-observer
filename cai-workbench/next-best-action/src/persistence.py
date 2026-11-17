"""Optional Trino persistence for NBA Phase 0 tables."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from .alert_types import NbaAlert
from .config import Settings
from .hitl import sitrep_link_for_alert
from .scoring import CoaScore
from .snapshot_bundle import SnapshotBundle
from . import trino_client


def _esc(value: str) -> str:
    return value.replace("'", "''")


def insert_incident(alert: NbaAlert, status: str, best_coa_id: str | None, settings: Settings) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    payload = _esc(json.dumps(alert.payload, ensure_ascii=False))
    best = "NULL" if best_coa_id is None else f"'{_esc(best_coa_id)}'"
    link = sitrep_link_for_alert(alert.payload, alert.flow_uuid, alert.priority)
    link_sql = "NULL" if not link else f"'{_esc(link)}'"
    sql = (
        f"INSERT INTO {settings.trino_catalog}.{settings.trino_schema}.nba_incidents "
        f"(flow_uuid, status, alert_type, priority, latitude, longitude, theatre, "
        f"created_at, updated_at, alert_payload, best_coa_id, sitrep_link, notes) VALUES ("
        f"'{_esc(alert.flow_uuid)}', '{_esc(status)}', '{_esc(alert.alert_type)}', "
        f"'{_esc(alert.priority)}', {alert.latitude}, {alert.longitude}, "
        f"'{_esc(alert.theatre)}', TIMESTAMP '{now}', TIMESTAMP '{now}', "
        f"'{payload}', {best}, {link_sql}, NULL)"
    )
    trino_client.execute_sql(sql)


def insert_snapshots(bundle: SnapshotBundle, settings: Settings) -> None:
    cap = bundle.captured_at.strftime("%Y-%m-%d %H:%M:%S.%f")
    for ref in bundle.snapshots:
        committed = (
            "NULL"
            if ref.committed_at is None
            else f"TIMESTAMP '{ref.committed_at}'"
        )
        sql = (
            f"INSERT INTO {settings.trino_catalog}.{settings.trino_schema}.nba_incident_snapshots "
            f"(flow_uuid, table_name, snapshot_id, committed_at, captured_at) VALUES ("
            f"'{_esc(bundle.flow_uuid)}', '{_esc(ref.table_name)}', {ref.snapshot_id}, "
            f"{committed}, TIMESTAMP '{cap}')"
        )
        trino_client.execute_sql(sql)


def insert_coa_scores(flow_uuid: str, ranked: list[CoaScore], settings: Settings) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    rank = 0
    for score in ranked:
        if not score.gates_passed:
            continue
        rank += 1
        detail = _esc(json.dumps(score.to_dict(), ensure_ascii=False))
        sql = (
            f"INSERT INTO {settings.trino_catalog}.{settings.trino_schema}.nba_coa_runs "
            f"(flow_uuid, coa_id, rank, score_total, score_latenz, score_entdeckung, "
            f"score_eskalation, score_kosten, gates_passed, detail_json, created_at) VALUES ("
            f"'{_esc(flow_uuid)}', '{_esc(score.coa.coa_id)}', {rank}, {score.score_total}, "
            f"{score.score_latenz}, {score.score_entdeckung}, {score.score_eskalation}, "
            f"{score.score_kosten}, TRUE, '{detail}', TIMESTAMP '{now}')"
        )
        trino_client.execute_sql(sql)