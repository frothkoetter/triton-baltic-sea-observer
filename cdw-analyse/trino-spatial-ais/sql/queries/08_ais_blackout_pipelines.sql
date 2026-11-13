-- Draft: AIS-Blackouts (30–1440 min) in Pipeline-Nähe / Überquerung im Dark-Modus
WITH ais_gaps AS (
    SELECT
        mmsi,
        zeitstempel AS blackout_start,
        LEAD(zeitstempel) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS blackout_ende,
        lon AS start_lon,
        lat AS start_lat,
        LEAD(lon) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_lon,
        LEAD(lat) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_lat,
        sog AS start_sog,
        LEAD(sog) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_sog,
        cog AS start_cog,
        LEAD(cog) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_cog
    FROM defense.ais_daten
),
verdaechtige_luecken AS (
    SELECT
        mmsi,
        blackout_start,
        blackout_ende,
        date_diff('minute', blackout_start, blackout_ende) AS luecke_minuten,
        start_lon,
        start_lat,
        end_lon,
        end_lat,
        start_sog,
        end_sog,
        ST_Distance(
            to_spherical_geography(ST_Point(start_lon, start_lat)),
            to_spherical_geography(ST_Point(end_lon, end_lat))
        ) / 1852.0 AS distanz_sm,
        (COALESCE(start_sog, 0) * (date_diff('minute', blackout_start, blackout_ende) / 60.0)) AS erwartete_distanz_sm,
        ST_Point(start_lon, start_lat) AS start_geom,
        ST_Point(end_lon, end_lat) AS end_geom,
        ST_LineFromText(
            'LINESTRING(' || CAST(start_lon AS varchar) || ' ' || CAST(start_lat AS varchar) || ', ' ||
            CAST(end_lon AS varchar) || ' ' || CAST(end_lat AS varchar) || ')'
        ) AS luecken_linie,
        LEAST(start_lon, end_lon) - 0.02 AS box_min_lon,
        GREATEST(start_lon, end_lon) + 0.02 AS box_max_lon,
        LEAST(start_lat, end_lat) - 0.02 AS box_min_lat,
        GREATEST(start_lat, end_lat) + 0.02 AS box_max_lat
    FROM ais_gaps
    WHERE end_lon IS NOT NULL
      AND date_diff('minute', blackout_start, blackout_ende) BETWEEN 30 AND 1440
      AND NOT (COALESCE(start_sog, 0) < 0.5 AND COALESCE(end_sog, 0) < 0.5)
),
pipeline_treffer AS (
    SELECT
        v.mmsi,
        p.pipeline_name,
        p.medium,
        v.blackout_start,
        v.blackout_ende,
        v.luecke_minuten,
        v.start_lon,
        v.start_lat,
        v.end_lon,
        v.end_lat,
        v.start_sog,
        v.end_sog,
        v.distanz_sm,
        v.erwartete_distanz_sm,
        MAX(CASE
            WHEN LEAST(
                ST_Distance(
                    to_spherical_geography(v.start_geom),
                    to_spherical_geography(ST_StartPoint(ST_GeometryFromText(p.seg_wkt)))
                ),
                ST_Distance(
                    to_spherical_geography(v.start_geom),
                    to_spherical_geography(ST_EndPoint(ST_GeometryFromText(p.seg_wkt)))
                )
            ) <= 2000 THEN 1 ELSE 0
        END) AS start_nah_an_pipeline,
        MAX(CASE
            WHEN LEAST(
                ST_Distance(
                    to_spherical_geography(v.end_geom),
                    to_spherical_geography(ST_StartPoint(ST_GeometryFromText(p.seg_wkt)))
                ),
                ST_Distance(
                    to_spherical_geography(v.end_geom),
                    to_spherical_geography(ST_EndPoint(ST_GeometryFromText(p.seg_wkt)))
                )
            ) <= 2000 THEN 1 ELSE 0
        END) AS ende_nah_an_pipeline,
        MAX(CASE
            WHEN ST_Intersects(v.luecken_linie, ST_GeometryFromText(p.seg_wkt)) THEN 1 ELSE 0
        END) AS kreuzt_waehrend_blackout
    FROM verdaechtige_luecken v
    JOIN defense.ostsee_pipelines_segmented p
      ON v.box_max_lon >= p.min_lon
     AND v.box_min_lon <= p.max_lon
     AND v.box_max_lat >= p.min_lat
     AND v.box_min_lat <= p.max_lat
     AND (
         ST_Intersects(v.luecken_linie, ST_GeometryFromText(p.seg_wkt))
         OR LEAST(
             ST_Distance(
                 to_spherical_geography(v.start_geom),
                 to_spherical_geography(ST_StartPoint(ST_GeometryFromText(p.seg_wkt)))
             ),
             ST_Distance(
                 to_spherical_geography(v.start_geom),
                 to_spherical_geography(ST_EndPoint(ST_GeometryFromText(p.seg_wkt)))
             )
         ) <= 2000
         OR LEAST(
             ST_Distance(
                 to_spherical_geography(v.end_geom),
                 to_spherical_geography(ST_StartPoint(ST_GeometryFromText(p.seg_wkt)))
             ),
             ST_Distance(
                 to_spherical_geography(v.end_geom),
                 to_spherical_geography(ST_EndPoint(ST_GeometryFromText(p.seg_wkt)))
             )
         ) <= 2000
     )
    WHERE p.status = 'In use'
    GROUP BY
        v.mmsi, p.pipeline_name, p.medium, v.blackout_start, v.blackout_ende,
        v.luecke_minuten, v.start_lon, v.start_lat, v.end_lon, v.end_lat,
        v.start_sog, v.end_sog, v.distanz_sm, v.erwartete_distanz_sm
)
SELECT
    mmsi,
    pipeline_name,
    medium,
    blackout_start,
    blackout_ende,
    luecke_minuten,
    ROUND(start_sog, 1) AS sog_vor_blackout_kn,
    ROUND(end_sog, 1) AS sog_nach_blackout_kn,
    ROUND(distanz_sm, 2) AS tatsaechliche_distanz_sm,
    ROUND(erwartete_distanz_sm, 2) AS erwartete_distanz_sm,
    ROUND(distanz_sm / NULLIF(erwartete_distanz_sm, 0), 2) AS distanz_verhaeltnis,
    CASE
        WHEN start_nah_an_pipeline = 1 AND ende_nah_an_pipeline = 1 THEN 'Stationärer Blackout direkt über Pipeline'
        WHEN start_nah_an_pipeline = 1 THEN 'AIS-Abschaltung direkt an Pipeline'
        WHEN ende_nah_an_pipeline = 1 THEN 'AIS-Reaktivierung direkt an Pipeline'
        WHEN kreuzt_waehrend_blackout = 1 THEN 'Pipeline-Überquerung im Dark-Modus'
        ELSE 'Sonstiger Korridor-Kontakt'
    END AS verdachtsmuster,
    start_lat,
    start_lon,
    end_lat,
    end_lon
FROM pipeline_treffer
ORDER BY
    (start_nah_an_pipeline + ende_nah_an_pipeline) DESC,
    luecke_minuten DESC;
