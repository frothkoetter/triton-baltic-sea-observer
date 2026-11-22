-- NBA: highest-priority DAS cable acoustic threat in the last 24h.
SELECT
    hadoop_file_url,
    segment_index,
    cable_asset,
    cable_name,
    chainage_m,
    segment_lat,
    segment_lon,
    threat_class,
    predicted_threat_type,
    model_confidence,
    filename,
    ingested_at
FROM {qualified}.das_cable_spectrogram_metadata
WHERE ingested_at >= current_timestamp - interval '24' hour
  AND segment_lat IS NOT NULL
  AND segment_lon IS NOT NULL
  AND (
      threat_class IN ('cui_sabotage_tap', 'anchor_drag', 'cable_strain_anomaly')
      OR predicted_threat_type IN ('cui_sabotage_tap', 'anchor_drag', 'cable_strain_anomaly')
  )
ORDER BY
    CASE COALESCE(predicted_threat_type, threat_class)
        WHEN 'cui_sabotage_tap' THEN 1
        WHEN 'anchor_drag' THEN 2
        WHEN 'cable_strain_anomaly' THEN 3
        ELSE 9
    END,
    COALESCE(model_confidence, 0) DESC,
    ingested_at DESC
LIMIT 1