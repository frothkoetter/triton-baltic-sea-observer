-- NBA Phase 4 — HITL columns (additive; safe on existing iceberg.defense.nba_incidents)
-- Apply: python3 run_trino_ddl.py sql/nba_schema_phase4_hitl.sql

USE iceberg.defense;

ALTER TABLE nba_incidents ADD COLUMN IF NOT EXISTS approved_coa_id varchar;
ALTER TABLE nba_incidents ADD COLUMN IF NOT EXISTS approved_at timestamp(6);
ALTER TABLE nba_incidents ADD COLUMN IF NOT EXISTS approved_by varchar;
ALTER TABLE nba_incidents ADD COLUMN IF NOT EXISTS sitrep_link varchar;