-- Draft §2 (vereinfacht): 500m-Puffer um WKT-Linie + SOG < 3 kn im Korridor
WITH infrastruktur_geo AS (
    SELECT
        id,
        name,
        medium,
        status,
        ST_GeometryFromText(geometrie_wkt) AS pipe_geom,
        ST_Buffer(ST_GeometryFromText(geometrie_wkt), 0.005) AS buffer_500m_geom
    FROM defense.ostsee_infrastruktur
    WHERE status = 'In use'
),
schiffs_segmente AS (
    SELECT
        mmsi,
        zeitstempel AS start_zeit,
        LEAD(zeitstempel) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_zeit,
        lon AS start_lon,
        lat AS start_lat,
        LEAD(lon) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_lon,
        LEAD(lat) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_lat,
        sog AS start_sog,
        LEAD(sog) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_sog
    FROM defense.ais_daten
),
fahrt_linien AS (
    SELECT
        mmsi,
        start_zeit,
        end_zeit,
        date_diff('second', start_zeit, end_zeit) / 60.0 AS dauer_minuten,
        (start_sog + COALESCE(end_sog, start_sog)) / 2.0 AS avg_sog,
        ST_LineFromText(
            'LINESTRING(' || CAST(start_lon AS varchar) || ' ' || CAST(start_lat AS varchar) || ', ' ||
            CAST(end_lon AS varchar) || ' ' || CAST(end_lat AS varchar) || ')'
        ) AS segment_geom
    FROM schiffs_segmente
    WHERE end_lon IS NOT NULL
      AND date_diff('minute', start_zeit, end_zeit) <= 120
)
SELECT
    f.mmsi,
    i.name AS infrastruktur_name,
    i.medium,
    f.start_zeit,
    f.end_zeit,
    ROUND(f.avg_sog, 2) AS avg_sog_kn,
    MAX(CASE WHEN ST_Intersects(f.segment_geom, i.pipe_geom) THEN 1 ELSE 0 END) AS ist_ueberquerung,
    MAX(CASE WHEN ST_Intersects(f.segment_geom, i.buffer_500m_geom) AND f.avg_sog < 3.0 THEN 1 ELSE 0 END) AS ist_langsamfahrt
FROM fahrt_linien f
JOIN infrastruktur_geo i
  ON ST_Intersects(f.segment_geom, i.buffer_500m_geom)
GROUP BY f.mmsi, i.name, i.medium, f.start_zeit, f.end_zeit, f.avg_sog, f.dauer_minuten
HAVING MAX(CASE WHEN ST_Intersects(f.segment_geom, i.pipe_geom) THEN 1 ELSE 0 END) = 1
    OR MAX(CASE WHEN ST_Intersects(f.segment_geom, i.buffer_500m_geom) AND f.avg_sog < 3.0 THEN 1 ELSE 0 END) = 1
ORDER BY f.start_zeit DESC;
