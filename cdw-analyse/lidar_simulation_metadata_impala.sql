-- Impala Iceberg: LiDAR vessel simulation metadata (NiFi LidarSimulator FlowFile attributes)
-- Also appended to create_db_tables_impala.sql (search: lidar_simulation_metadata)

CREATE DATABASE IF NOT EXISTS defense;
USE defense;

DROP TABLE IF EXISTS lidar_simulation_metadata;
CREATE TABLE lidar_simulation_metadata (
  hadoop_file_url STRING COMMENT 'Object store URI of the LAS file (NiFi attribute hadoop.file.url), e.g. s3a://bucket/path/object.',
  center_lat DOUBLE COMMENT 'Vessel scene center latitude in WGS84 (lidar.center.lat).',
  center_lon DOUBLE COMMENT 'Vessel scene center longitude in WGS84 (lidar.center.lon).',
  classification INT COMMENT 'LAS point class code for vessel type (lidar.classification).',
  filename STRING COMMENT 'LAS basename written to object store (lidar.filename).',
  heading_deg DOUBLE COMMENT 'Vessel heading in degrees clockwise from north (lidar.heading_deg).',
  point_count BIGINT COMMENT 'Number of points in the LAS file (lidar.point_count).',
  vessel_name STRING COMMENT 'Display name of the simulated vessel (lidar.vessel_name).',
  vessel_type STRING COMMENT 'Simulator vessel type key, e.g. corvette (lidar.vessel_type).',
  ingested_at TIMESTAMP COMMENT 'When the metadata row was recorded (set at ingest).'
)
COMMENT 'Metadata for Baltic LiDAR vessel simulation LAS files'
PARTITIONED BY SPEC (day(ingested_at))
STORED BY ICEBERG;

-- Optional sample row
-- INSERT INTO lidar_simulation_metadata (
--   hadoop_file_url, center_lat, center_lon, classification,
--   filename, heading_deg, point_count, vessel_name, vessel_type, ingested_at
-- ) VALUES (
--   's3a://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/5c25859f-a527-4b34-8e73-654173d861b5',
--   59.25538758251504, 23.018171690628463, 6,
--   'baltic_fleet.las', 266.0833353866533, 45000,
--   'Corvette (Project 20380)', 'corvette', CAST(NOW() AS TIMESTAMP)
-- );
