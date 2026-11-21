-- Impala Iceberg: DAS cable spectrogram segment metadata (NiFi DasCableAcousticSimulator)
-- One row per HDF5 spectrogram window along a cable corridor (segment_index in batch file).
-- Ontology: DasCableSpectrogramMetadata — ontology/mappings/das_cable_spectrogram_metadata.yaml

CREATE DATABASE IF NOT EXISTS defense;
USE defense;

DROP TABLE IF EXISTS das_cable_spectrogram_metadata;
CREATE TABLE das_cable_spectrogram_metadata (
  hadoop_file_url STRING COMMENT 'Object store URI of the HDF5 batch (hadoop.file.url), e.g. s3a://bucket/path/batch.h5.',
  segment_index INT COMMENT 'Zero-based segment index inside the HDF5 batch (das.segment.index).',
  cable_asset STRING COMMENT 'Cable registry key (das.cable.asset), e.g. BALTICCONNECTOR.',
  cable_id STRING COMMENT 'Stable cable identifier (das.cable.id).',
  cable_name STRING COMMENT 'Human-readable cable name (das.cable.name).',
  chainage_m DOUBLE COMMENT 'Along-cable distance in meters (das.chainage_m).',
  segment_lat DOUBLE COMMENT 'Segment center latitude WGS84 (das.segment.lat).',
  segment_lon DOUBLE COMMENT 'Segment center longitude WGS84 (das.segment.lon).',
  threat_class STRING COMMENT 'Ground-truth simulator label (das.threat_class), e.g. anchor_drag.',
  predicted_threat_type STRING COMMENT 'ML-predicted threat label (inference pipeline).',
  model_confidence DOUBLE COMMENT 'Model confidence for predicted_threat_type (0.0–1.0).',
  channel_count INT COMMENT 'DAS channels in segment (das.channel.count).',
  window_seconds DOUBLE COMMENT 'Time window length in seconds (das.window.seconds).',
  fs_hz DOUBLE COMMENT 'Sample rate Hz (das.fs_hz).',
  spectral_peak_hz DOUBLE COMMENT 'Dominant spectral peak Hz (optional feature).',
  energy_rms DOUBLE COMMENT 'RMS strain-rate energy (optional feature).',
  filename STRING COMMENT 'HDF5 basename (filename).',
  ingested_at TIMESTAMP COMMENT 'When the metadata row was recorded (set at ingest).'
)
PARTITIONED BY SPEC (day(ingested_at))
STORED BY ICEBERG;