-- Draft: Mehrfach-Überquerungen & Langsamfahrt mit ostsee_pipelines_segmented + BBox-Vorfilter
WITH schiffs_segmente AS (
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
        LEAST(start_lon, end_lon) AS ship_min_lon,
        GREATEST(start_lon, end_lon) AS ship_max_lon,
        LEAST(start_lat, end_lat) AS ship_min_lat,
        GREATEST(start_lat, end_lat) AS ship_max_lat,
        ST_LineFromText(
            'LINESTRING(' || CAST(start_lon AS varchar) || ' ' || CAST(start_lat AS varchar) || ', ' ||
            CAST(end_lon AS varchar) || ' ' || CAST(end_lat AS varchar) || ')'
        ) AS segment_geom
    FROM schiffs_segmente
    WHERE end_lon IS NOT NULL
      AND date_diff('minute', start_zeit, end_zeit) <= 120
),
korridor_ereignisse AS (
    SELECT
        f.mmsi,
        p.pipeline_name,
        p.medium,
        f.start_zeit,
        f.end_zeit,
        f.dauer_minuten,
        f.avg_sog,
        MAX(CASE WHEN ST_Intersects(f.segment_geom, ST_GeometryFromText(p.seg_wkt)) THEN 1 ELSE 0 END) AS ist_ueberquerung,
        MAX(CASE WHEN f.avg_sog < 3.0 THEN 1 ELSE 0 END) AS ist_langsamfahrt
    FROM fahrt_linien f
    JOIN defense.ostsee_pipelines_segmented p
      ON f.ship_max_lon >= p.min_lon
     AND f.ship_min_lon <= p.max_lon
     AND f.ship_max_lat >= p.min_lat
     AND f.ship_min_lat <= p.max_lat
     AND ST_Intersects(f.segment_geom, ST_Buffer(ST_GeometryFromText(p.seg_wkt), 0.005))
    WHERE p.status = 'In use'
    GROUP BY f.mmsi, p.pipeline_name, p.medium, f.start_zeit, f.end_zeit, f.dauer_minuten, f.avg_sog
)
SELECT
    mmsi,
    pipeline_name,
    medium,
    SUM(ist_ueberquerung) AS anzahl_exakte_ueberquerungen,
    SUM(ist_langsamfahrt) AS segmente_unter_3_knoten,
    ROUND(SUM(CASE WHEN ist_langsamfahrt = 1 THEN dauer_minuten ELSE 0 END), 1) AS minuten_langsam_im_korridor,
    ROUND(MIN(avg_sog), 2) AS min_geschwindigkeit_kn,
    MIN(start_zeit) AS erster_kontakt,
    MAX(end_zeit) AS letzter_kontakt
FROM korridor_ereignisse
GROUP BY mmsi, pipeline_name, medium
HAVING SUM(ist_ueberquerung) > 1
    OR SUM(CASE WHEN ist_langsamfahrt = 1 THEN dauer_minuten ELSE 0 END) > 15
ORDER BY minuten_langsam_im_korridor DESC, anzahl_exakte_ueberquerungen DESC;
