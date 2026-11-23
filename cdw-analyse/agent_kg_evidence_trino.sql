-- Agent console / Semantica KG evidence queries (read-only).
-- Run on CML: python3 -c "..." or trino CLI with iceberg.defense session.
-- UI allowlist: triton-ui/server/agent-kg.js (IDs kg_*).

-- kg_das_threat_gt
SELECT threat_class AS label, COUNT(*) AS n
FROM iceberg.defense.das_cable_spectrogram_metadata
WHERE threat_class IS NOT NULL
GROUP BY 1
ORDER BY n DESC
LIMIT 20;

-- kg_das_threat_ml
SELECT predicted_threat_type AS label, COUNT(*) AS n
FROM iceberg.defense.das_cable_spectrogram_metadata
WHERE predicted_threat_type IS NOT NULL
GROUP BY 1
ORDER BY n DESC
LIMIT 20;

-- kg_outliers_by_type
SELECT outlier_type AS label, COUNT(*) AS n
FROM iceberg.defense.vessel_outliers
GROUP BY 1
ORDER BY n DESC
LIMIT 16;

-- kg_lagebild_sources
SELECT "Data_Source" AS source, COUNT(*) AS n
FROM iceberg.defense.lagebild
GROUP BY 1
ORDER BY n DESC
LIMIT 20;

-- kg_nba_open_incidents
SELECT flow_uuid, alert_type, priority, status, theatre, best_coa_id, updated_at
FROM iceberg.defense.nba_incidents
ORDER BY updated_at DESC
LIMIT 10;