-- MP-1: mission readiness excerpt on incident (denormalized from decision_record §10)
ALTER TABLE nba_incidents ADD COLUMN IF NOT EXISTS mission_readiness_json varchar;