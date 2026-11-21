-- =========================================================================================
-- Extended CDW analytics: DAS cable spectrogram metadata (parallel to LiDAR LAS catalog)
-- Catalog/schema: iceberg.defense
-- Prereq: defense.das_cable_spectrogram_metadata populated from NiFi + ML inference.
-- =========================================================================================

USE iceberg.defense;

-- Threat mix by cable asset (24h)
SELECT
    cable_asset,
    cable_name,
    threat_class,
    COUNT(*) AS segment_cnt,
    AVG(COALESCE(model_confidence, 0)) AS avg_ml_confidence,
    MAX(ingested_at) AS last_ingest
FROM defense.das_cable_spectrogram_metadata
WHERE ingested_at >= current_timestamp - interval '24' hour
GROUP BY 1, 2, 3
ORDER BY cable_asset, segment_cnt DESC;

-- High-interest segments within 2 km of in-use pipeline segment geometry
WITH das_recent AS (
    SELECT *
    FROM defense.das_cable_spectrogram_metadata
    WHERE ingested_at >= current_timestamp - interval '24' hour
      AND segment_lat IS NOT NULL
      AND segment_lon IS NOT NULL
)
SELECT
    d.cable_asset,
    d.cable_name,
    d.threat_class,
    d.predicted_threat_type,
    d.model_confidence,
    d.chainage_m,
    d.segment_lat,
    d.segment_lon,
    p.pipeline_name,
    p.medium,
    MIN(
        LEAST(
            ST_Distance(
                to_spherical_geography(ST_Point(d.segment_lon, d.segment_lat)),
                to_spherical_geography(ST_StartPoint(ST_GeometryFromText(p.seg_wkt)))
            ),
            ST_Distance(
                to_spherical_geography(ST_Point(d.segment_lon, d.segment_lat)),
                to_spherical_geography(ST_EndPoint(ST_GeometryFromText(p.seg_wkt)))
            )
        )
    ) AS min_distance_m
FROM das_recent d
JOIN defense.ostsee_pipelines_segmented p
  ON p.status = 'In use'
WHERE d.threat_class IN ('anchor_drag', 'cui_sabotage_tap', 'cable_strain_anomaly', 'trawler_scrape')
   OR d.predicted_threat_type IN ('anchor_drag', 'cui_sabotage_tap', 'cable_strain_anomaly', 'trawler_scrape')
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10
HAVING MIN(
    LEAST(
        ST_Distance(
            to_spherical_geography(ST_Point(d.segment_lon, d.segment_lat)),
            to_spherical_geography(ST_StartPoint(ST_GeometryFromText(p.seg_wkt)))
        ),
        ST_Distance(
            to_spherical_geography(ST_Point(d.segment_lon, d.segment_lat)),
            to_spherical_geography(ST_EndPoint(ST_GeometryFromText(p.seg_wkt)))
        )
    )
) <= 2000
ORDER BY min_distance_m, d.chainage_m;

-- ML vs ground truth (when both present)
SELECT
    threat_class AS gt_label,
    predicted_threat_type AS ml_label,
    COUNT(*) AS n,
    AVG(model_confidence) AS avg_confidence
FROM defense.das_cable_spectrogram_metadata
WHERE ingested_at >= current_timestamp - interval '7' day
  AND predicted_threat_type IS NOT NULL
GROUP BY 1, 2
ORDER BY n DESC;