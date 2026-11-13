-- Draft §2: AIS-Trajektorien (LEAD-Segmente) vs. Pipeline-WKT (ST_Intersects)
WITH schiffs_segmente AS (
    SELECT
        mmsi,
        zeitstempel AS start_zeit,
        LEAD(zeitstempel) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_zeit,
        lon AS start_lon,
        lat AS start_lat,
        LEAD(lon) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_lon,
        LEAD(lat) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_lat
    FROM defense.ais_daten
),
fahrt_linien AS (
    SELECT
        mmsi,
        start_zeit,
        end_zeit,
        ST_LineFromText(
            'LINESTRING(' || CAST(start_lon AS varchar) || ' ' || CAST(start_lat AS varchar) || ', ' ||
            CAST(end_lon AS varchar) || ' ' || CAST(end_lat AS varchar) || ')'
        ) AS segment_geom
    FROM schiffs_segmente
    WHERE end_lon IS NOT NULL
      AND end_lat IS NOT NULL
      AND date_diff('minute', start_zeit, end_zeit) <= 120
),
infrastruktur_geo AS (
    SELECT
        id,
        name,
        typ,
        ST_GeometryFromText(geometrie_wkt) AS pipe_geom
    FROM defense.ostsee_infrastruktur
)
SELECT
    f.mmsi,
    i.name AS infrastruktur_name,
    i.typ,
    COUNT(*) AS anzahl_ueberquerungen,
    MIN(f.start_zeit) AS erste_ueberquerung,
    MAX(f.end_zeit) AS letzte_ueberquerung
FROM fahrt_linien f
JOIN infrastruktur_geo i
  ON ST_Intersects(f.segment_geom, i.pipe_geom)
GROUP BY f.mmsi, i.name, i.typ
HAVING COUNT(*) > 1
ORDER BY anzahl_ueberquerungen DESC;
