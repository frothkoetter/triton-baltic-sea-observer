-- Mission execution: dynamic target tracking + STANAG/buoy tagging (additive)
-- Apply: python3 run_trino_ddl.py sql/nba_schema_mission_tracking.sql --continue

USE iceberg.defense;

-- Core orders (if nba_schema_mission_execution.sql not yet applied, run that first)
CREATE TABLE IF NOT EXISTS nba_mission_orders (
    mission_id varchar COMMENT 'UUID primary key',
    flow_uuid varchar COMMENT 'NBA incident',
    approved_coa_id varchar COMMENT 'A-D',
    mission_type varchar COMMENT 'COMBINED, BUOY_TRACK_ONLY, SURFACE_DISPATCH',
    status varchar COMMENT 'QUEUED, ACTIVE, ON_STATION, COMPLETED, ABORTED, FAILED',
    target_latitude double COMMENT 'Initial / last known target lat',
    target_longitude double COMMENT 'Initial / last known target lon',
    target_mode varchar COMMENT 'STATIC_POINT, VESSEL_MMSI, CORRELATED_OBJECT',
    target_mmsi varchar COMMENT 'Dynamic track AIS key',
    target_correlation_id varchar COMMENT 'Sonar / MDA correlation',
    track_duration_min integer COMMENT 'Dynamic follow window',
    target_radius_km double COMMENT 'Standoff / ring default',
    playbook_version varchar COMMENT 'mission_nato_playbook.json version',
    parameters_json varchar COMMENT 'COA playbook + formations + STANAG',
    planned_start timestamp(6) COMMENT 'Optional',
    started_at timestamp(6) COMMENT 'Simulation start',
    ended_at timestamp(6) COMMENT 'Simulation end',
    issued_by varchar COMMENT 'Operator / nba-auto',
    issued_at timestamp(6) COMMENT 'Order time',
    updated_at timestamp(6) COMMENT 'Last status change',
    notes varchar COMMENT 'Free text'
);

CREATE TABLE IF NOT EXISTS nba_mission_events (
    event_id varchar COMMENT 'UUID',
    mission_id varchar COMMENT 'Parent mission',
    event_ts timestamp(6) COMMENT 'UTC',
    event_type varchar COMMENT 'ORDER_CREATED, TARGET_REFRESH, BUOY_DEPLOYED, STANAG_SENT, …',
    payload_json varchar COMMENT 'MMSI, buoyid, message_id, lat/lon',
    source varchar COMMENT 'nifi, ui, nba'
);

CREATE TABLE IF NOT EXISTS nba_mission_track_samples (
    mission_id varchar COMMENT 'Parent mission',
    sample_ts timestamp(6) COMMENT 'Target observation time',
    target_latitude double COMMENT 'Tracked lat',
    target_longitude double COMMENT 'Tracked lon',
    target_cog_deg double COMMENT 'COG if known',
    target_sog_kn double COMMENT 'SOG if known',
    target_mmsi varchar COMMENT 'AIS key',
    source varchar COMMENT 'ais_events_ice, dead_reckon, static'
) COMMENT 'Time series for dynamic target tracking (Mission Control map)';

-- Tagging on existing fact tables (nullable, backward compatible)
ALTER TABLE buoy_data ADD COLUMN IF NOT EXISTS mission_id varchar;
ALTER TABLE buoy_data ADD COLUMN IF NOT EXISTS einsatz_id varchar;
ALTER TABLE buoy_data ADD COLUMN IF NOT EXISTS deployment_role varchar;
ALTER TABLE buoy_data ADD COLUMN IF NOT EXISTS playbook_sensor_profile varchar;

ALTER TABLE maritime_surveillance_reports ADD COLUMN IF NOT EXISTS mission_id varchar;
ALTER TABLE maritime_surveillance_reports ADD COLUMN IF NOT EXISTS einsatz_id varchar;
ALTER TABLE maritime_surveillance_reports ADD COLUMN IF NOT EXISTS buoy_id varchar;
ALTER TABLE maritime_surveillance_reports ADD COLUMN IF NOT EXISTS target_correlation_id varchar;