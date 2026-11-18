-- NBA Phase 6 — materialized fact copies per incident (registry)
-- Apply: python3 run_trino_ddl.py sql/nba_schema_phase6_materialize.sql

USE iceberg.defense;

DROP TABLE IF EXISTS nba_materialized_tables;

CREATE TABLE nba_materialized_tables (
    flow_uuid varchar COMMENT 'Incident id',
    source_table varchar COMMENT 'Origin Iceberg table',
    mat_table varchar COMMENT 'nba_mat_<flow>_<table> copy',
    snapshot_id bigint COMMENT 'FOR VERSION AS OF id used',
    row_limit integer COMMENT 'ROWS limited for large facts',
    created_at timestamp(6) COMMENT 'UTC materialization time'
);