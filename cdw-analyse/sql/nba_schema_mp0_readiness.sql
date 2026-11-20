-- MP-0: optional staging for SAP readiness snapshots (batch via CPI / Datasphere)
CREATE TABLE IF NOT EXISTS sap_readiness_staging (
  force_element_id varchar,
  name varchar,
  readiness_code varchar,
  capabilities varchar,
  rotation_conflict boolean,
  fuel_days_remaining integer,
  munition_gate varchar,
  theatre varchar,
  snapshot_at timestamp(6) with time zone,
  source_system varchar
);