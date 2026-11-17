-- NBA Phase 0 — Iceberg tables in iceberg.defense
-- Apply: set -a && source ../.env.trino.local && set +a
--       python3 ../run_trino_ddl.py nba_schema_trino.sql

USE iceberg.defense;

DROP TABLE IF EXISTS nba_coa_runs;
DROP TABLE IF EXISTS nba_incident_snapshots;
DROP TABLE IF EXISTS nba_incidents;

CREATE TABLE nba_incidents (
    flow_uuid varchar COMMENT 'Unique incident / workflow id',
    status varchar COMMENT 'NEW, SNAPSHOT_READY, SCORED, CLOSED',
    alert_type varchar COMMENT 'e.g. demo_greifswald, ais_blackout',
    priority varchar COMMENT 'ROUTINE, PRIORITY, FLASH',
    latitude double COMMENT 'Incident latitude (WGS84)',
    longitude double COMMENT 'Incident longitude (WGS84)',
    theatre varchar COMMENT 'Human-readable area label',
    created_at timestamp(6) COMMENT 'UTC row creation',
    updated_at timestamp(6) COMMENT 'UTC last update',
    alert_payload varchar COMMENT 'JSON alert context',
    best_coa_id varchar COMMENT 'Winning COA id after scoring',
    approved_coa_id varchar COMMENT 'Operator-approved COA (Phase 4 HITL)',
    approved_at timestamp(6) COMMENT 'UTC approval time',
    approved_by varchar COMMENT 'Operator or role id',
    sitrep_link varchar COMMENT 'Soft link: SITREP precedence or ESCALATED line',
    notes varchar COMMENT 'Optional operator notes'
);

CREATE TABLE nba_incident_snapshots (
    flow_uuid varchar,
    table_name varchar COMMENT 'Iceberg fact table name',
    snapshot_id bigint COMMENT 'Iceberg snapshot id at incident time',
    committed_at timestamp(6) WITH TIME ZONE COMMENT 'Snapshot commit time from $snapshots',
    captured_at timestamp(6) COMMENT 'UTC capture time in NBA pipeline'
);

CREATE TABLE nba_coa_runs (
    flow_uuid varchar,
    coa_id varchar COMMENT 'A, B, C, D, …',
    rank integer COMMENT '1 = best after scoring',
    score_total double,
    score_latenz double,
    score_entdeckung double,
    score_eskalation double,
    score_kosten double,
    gates_passed boolean,
    detail_json varchar COMMENT 'JSON breakdown',
    created_at timestamp(6)
);