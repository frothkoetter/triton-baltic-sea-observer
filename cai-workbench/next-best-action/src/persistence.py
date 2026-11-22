"""Optional Trino persistence for NBA Phase 0 tables."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from .alert_types import NbaAlert
from .config import Settings
from .hitl import sitrep_link_for_alert
from .scoring import CoaScore
from .materialize import MaterializeResult
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
    """Persist full COA matrix (incl. gate failures) for HITL / Lagebild."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    pass_rank = 0
    for score in ranked:
        if score.gates_passed:
            pass_rank += 1
        rank_val = pass_rank if score.gates_passed else 0
        gates_sql = "TRUE" if score.gates_passed else "FALSE"
        detail = _esc(json.dumps(score.to_dict(), ensure_ascii=False))
        sql = (
            f"INSERT INTO {settings.trino_catalog}.{settings.trino_schema}.nba_coa_runs "
            f"(flow_uuid, coa_id, rank, score_total, score_latenz, score_entdeckung, "
            f"score_eskalation, score_kosten, gates_passed, detail_json, created_at) VALUES ("
            f"'{_esc(flow_uuid)}', '{_esc(score.coa.coa_id)}', {rank_val}, {score.score_total}, "
            f"{score.score_latenz}, {score.score_entdeckung}, {score.score_eskalation}, "
            f"{score.score_kosten}, {gates_sql}, '{detail}', TIMESTAMP '{now}')"
        )
        trino_client.execute_sql(sql)


def update_decision_artifacts(
    flow_uuid: str,
    *,
    decision_record: dict,
    grounding: dict | None,
    settings: Settings,
    shacl_report: dict | None = None,
    mission_readiness: dict | None = None,
) -> None:
    """Phase 5: persist full decision card + grounding on nba_incidents."""
    from .shacl_gate import (
        ShaclGateBlocked,
        ShaclReport,
        assert_shacl_conforms,
        validate_decision_artifacts,
    )

    if settings.shacl_gate:
        if shacl_report is not None:
            report = ShaclReport(
                conforms=bool(shacl_report.get("conforms")),
                report_text=shacl_report.get("report_excerpt", ""),
                violations=list(shacl_report.get("violations") or []),
                engine=str(shacl_report.get("engine") or "artifact"),
            )
        else:
            _, report = validate_decision_artifacts(decision_record, grounding)
        try:
            assert_shacl_conforms(report, gate_enabled=True)
        except ShaclGateBlocked:
            raise
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    decision_json = _esc(json.dumps(decision_record, ensure_ascii=False))
    grounding_sql = "NULL"
    if grounding:
        grounding_sql = f"'{_esc(json.dumps(grounding, ensure_ascii=False))}'"
    readiness_sql = ""
    if mission_readiness:
        readiness_json = _esc(json.dumps(mission_readiness, ensure_ascii=False))
        readiness_sql = f", mission_readiness_json = '{readiness_json}'"
    sql = (
        f"UPDATE {settings.trino_catalog}.{settings.trino_schema}.nba_incidents "
        f"SET decision_record_json = '{decision_json}', "
        f"grounding_json = {grounding_sql}{readiness_sql}, "
        f"updated_at = TIMESTAMP '{now}' "
        f"WHERE flow_uuid = '{_esc(flow_uuid)}'"
    )
    try:
        trino_client.execute_sql(sql)
    except Exception:
        if mission_readiness:
            sql = (
                f"UPDATE {settings.trino_catalog}.{settings.trino_schema}.nba_incidents "
                f"SET decision_record_json = '{decision_json}', "
                f"grounding_json = {grounding_sql}, "
                f"updated_at = TIMESTAMP '{now}' "
                f"WHERE flow_uuid = '{_esc(flow_uuid)}'"
            )
        trino_client.execute_sql(sql)


_DDL_HINT = (
    "One-time Trino DDL (CML session with TRINO_USER/TRINO_PASSWORD, e.g. .env.trino.local):\n"
    "  cd cdw-analyse && set -a && source .env.trino.local && set +a\n"
    "  bash sql/nba_apply_all_ddl.sh\n"
    "Phase 0 only: python3 run_trino_ddl.py sql/nba_schema_trino.sql"
)


def nba_core_tables_ready(settings: Settings) -> bool:
    """Return False when base NBA Iceberg tables were never created."""
    fq = f"{settings.trino_catalog}.{settings.trino_schema}"
    try:
        trino_client.fetch_all(f"SELECT flow_uuid FROM {fq}.nba_incidents LIMIT 1")
        return True
    except Exception as exc:  # noqa: BLE001 — Trino table missing
        msg = str(exc)
        if "TABLE_NOT_FOUND" in msg or "does not exist" in msg.lower():
            return False
        raise


def require_nba_core_tables(settings: Settings) -> None:
    """Fail fast before a long pipeline run when persist is enabled."""
    if not nba_core_tables_ready(settings):
        fq = f"{settings.trino_catalog}.{settings.trino_schema}"
        raise RuntimeError(
            f"Trino table {fq}.nba_incidents does not exist.\n{_DDL_HINT}"
        )


def materialized_registry_ready(settings: Settings) -> bool:
    """Return False when Phase 6 DDL was not applied on Trino."""
    fq = f"{settings.trino_catalog}.{settings.trino_schema}"
    try:
        trino_client.fetch_all(
            f"SELECT flow_uuid FROM {fq}.nba_materialized_tables LIMIT 1"
        )
        return True
    except Exception as exc:  # noqa: BLE001 — Trino table missing
        msg = str(exc)
        if "TABLE_NOT_FOUND" in msg or "does not exist" in msg.lower():
            return False
        raise


def insert_materialized_tables(
    flow_uuid: str,
    results: list[MaterializeResult],
    settings: Settings,
) -> None:
    if not results:
        return
    if not materialized_registry_ready(settings):
        import sys

        print(
            "NBA: skipping nba_materialized_tables INSERT — table missing. "
            f"Apply cdw-analyse/sql/nba_schema_phase6_materialize.sql (or nba_apply_all_ddl.sh).\n{_DDL_HINT}",
            file=sys.stderr,
        )
        return
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    fq = f"{settings.trino_catalog}.{settings.trino_schema}"
    for r in results:
        sql = (
            f"INSERT INTO {fq}.nba_materialized_tables "
            f"(flow_uuid, source_table, mat_table, snapshot_id, row_limit, created_at) "
            f"VALUES ("
            f"'{_esc(flow_uuid)}', '{_esc(r.source_table)}', '{_esc(r.mat_table)}', "
            f"{r.snapshot_id}, {r.row_limit}, TIMESTAMP '{now}')"
        )
        trino_client.execute_sql(sql)