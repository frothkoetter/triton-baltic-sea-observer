-- Segmentierte Pipeline-Abschnitte (WKT varchar + vorberechnete BBox — Iceberg-kompatibel)
USE iceberg.defense;

DROP TABLE IF EXISTS ostsee_pipelines_segmented;
CREATE TABLE ostsee_pipelines_segmented AS
WITH pipeline_geom AS (
    SELECT
        pipeline_name,
        medium,
        status,
        ST_GeometryFromText(geometrie_wkt) AS geom
    FROM ostsee_pipelines
),
single_lines AS (
    SELECT
        pipeline_name,
        medium,
        status,
        ST_GeometryN(geom, CAST(idx AS integer)) AS line_geom
    FROM pipeline_geom
    CROSS JOIN UNNEST(sequence(1, GREATEST(ST_NumGeometries(geom), 1))) AS t(idx)
    WHERE geom IS NOT NULL
),
line_segments AS (
    SELECT
        pipeline_name,
        medium,
        status,
        ST_LineFromText(
            'LINESTRING(' ||
            CAST(ST_X(ST_PointN(line_geom, CAST(pt_idx AS integer))) AS varchar) || ' ' ||
            CAST(ST_Y(ST_PointN(line_geom, CAST(pt_idx AS integer))) AS varchar) || ', ' ||
            CAST(ST_X(ST_PointN(line_geom, CAST(pt_idx + 1 AS integer))) AS varchar) || ' ' ||
            CAST(ST_Y(ST_PointN(line_geom, CAST(pt_idx + 1 AS integer))) AS varchar) || ')'
        ) AS seg_geom
    FROM single_lines
    CROSS JOIN UNNEST(sequence(1, GREATEST(ST_NumPoints(line_geom) - 1, 0))) AS t(pt_idx)
    WHERE ST_NumPoints(line_geom) >= 2
)
SELECT
    pipeline_name,
    medium,
    status,
  ST_AsText(seg_geom) AS seg_wkt,
    ST_XMin(ST_Buffer(seg_geom, 0.005)) AS min_lon,
    ST_XMax(ST_Buffer(seg_geom, 0.005)) AS max_lon,
    ST_YMin(ST_Buffer(seg_geom, 0.005)) AS min_lat,
    ST_YMax(ST_Buffer(seg_geom, 0.005)) AS max_lat
FROM line_segments
WHERE seg_geom IS NOT NULL;
