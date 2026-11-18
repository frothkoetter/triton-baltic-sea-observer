-- NBA Phase 5 — decision record + grounding persisted for HITL (additive)
-- Apply: python3 run_trino_ddl.py sql/nba_schema_phase5_decision.sql

USE iceberg.defense;

ALTER TABLE nba_incidents ADD COLUMN IF NOT EXISTS decision_record_json varchar;
ALTER TABLE nba_incidents ADD COLUMN IF NOT EXISTS grounding_json varchar;