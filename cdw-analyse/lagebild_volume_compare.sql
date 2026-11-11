-- Compare row volume: snapshot (latest per entity+harbour) vs full 24h event stream.
-- Run on Trino/Iceberg after deploying views from create_db_tables_trino.sql.

-- Total counts (expect lagebild_all_events >= lagebild; often much higher for AIS/Marine/RUS_Fleet)
-- SELECT count(*) AS snapshot_rows FROM iceberg.defense.lagebild;
-- SELECT count(*) AS all_events_rows FROM iceberg.defense.lagebild_all_events;

-- By feed (snapshot is capped at ~1 row per entity per harbour; stream reflects update frequency)
-- SELECT Data_Source, count(*) AS n FROM iceberg.defense.lagebild GROUP BY 1 ORDER BY 2 DESC;
-- SELECT Data_Source, count(*) AS n FROM iceberg.defense.lagebild_all_events GROUP BY 1 ORDER BY 2 DESC;

-- Ratio quick check (all / snapshot); NULL if snapshot empty
-- SELECT
--     a.Data_Source,
--     s.n AS snapshot_n,
--     a.n AS all_events_n,
--     round(a.n * 1.0 / nullif(s.n, 0), 2) AS volume_ratio
-- FROM (
--     SELECT Data_Source, count(*) AS n FROM iceberg.defense.lagebild_all_events GROUP BY 1
-- ) a
-- LEFT JOIN (
--     SELECT Data_Source, count(*) AS n FROM iceberg.defense.lagebild GROUP BY 1
-- ) s ON a.Data_Source = s.Data_Source
-- ORDER BY volume_ratio DESC NULLS LAST;
