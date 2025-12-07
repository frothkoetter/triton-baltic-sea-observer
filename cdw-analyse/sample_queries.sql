/* 
The provided SQL query selects specific columns from the tables ais_events_ice and 
baltic_sea_harbours. It joins these two tables based on the condition that the distance 
between the points defined by their longitude and latitude is less than 10. The query then 
filters the results to only include rows where the country in the baltic_sea_harbours 
table is 'Germany'. Finally, it orders the results by the event_timestamp column in 
descending order and limits the output to 100 rows.
  */
SELECT a.mmsi, a.event_timestamp, a.latitude,b.latitude, a.longitude, b.longitude
FROM defense.ais_events_ice a
JOIN defense.baltic_sea_harbours b ON ST_Distance(ST_Point(a.longitude, a.latitude), ST_Point(b.longitude, b.latitude)) < 10
WHERE b.country = 'Germany'
ORDER BY a.event_timestamp DESC
LIMIT 100;



/* 
The SQL query joins three tables: ais_events_ice, buoy_data, and ships. It calculates the 
distance in kilometers between AIS positions of ships and buoy positions at the same 
timestamp. The query includes the mmsi, event timestamp, latitude, and longitude of the 
AIS positions, the buoy id, timestamp, latitude, and longitude of the buoy positions, and 
the ship name. It filters the results based on the payload object type being 
'SURFACE_VESSEL' and a distance less than or equal to 1 kilometer.

  */
-- Berechnung der Distanz in Kilometern (ST_Distance_Sphere liefert Meter, muss durch 1000 geteilt werden)
/* NQL: add ship_name */
/* NQL: add ship_name */
SELECT
    a.mmsi,
    a.event_timestamp AS ais_zeitstempel,
    a.latitude AS ais_breitengrad,
    a.longitude AS ais_längengrad,
    b.buoyid,
    b.ts AS bojen_zeitstempel,
    b.geo_position_lat AS bojen_breitengrad,
    b.geo_position_lon AS bojen_längengrad,
    s.ship_name,

    ST_Distance(
        ST_Point(a.longitude, a.latitude),
        ST_Point(b.geo_position_lon, b.geo_position_lat)
    )  AS distance_km
FROM
    defense.ais_events_ice a
JOIN defense.buoy_data b ON (
        DATE_TRUNC('minute', CAST(a.event_timestamp AS TIMESTAMP)) = DATE_TRUNC('minute', CAST(b.ts AS TIMESTAMP))
    )
JOIN defense.ships s ON a.mmsi = s.mmsi
WHERE
    b.payload_object_type = 'SURFACE_VESSEL'
    AND
    ST_Distance(
        ST_Point(a.longitude, a.latitude),
        ST_Point(b.geo_position_lon, b.geo_position_lat)
    ) <= 1;




-- CTE 1: Vorbereitung der Bojendaten
WITH BuoyDetections AS (
    SELECT
        buoyid,
        CAST(ts AS TIMESTAMP) AS buoy_time,
        geo_position_lat AS buoy_lat, 
        geo_position_lon AS buoy_lon,
        payload_object_classification,
        payload_magneticField_anomaly 
    FROM
        defense.buoy_data 
    WHERE
        payload_detectionConfidence IS NOT NULL 
),

-- CTE 2: Korrelation mit zivilem AIS-Verkehr und sanktionierten Schiffen
AisCorrelations AS (
    SELECT
        b.buoyid,
        b.buoy_time,
        b.buoy_lat,
        b.buoy_lon,
        b.payload_object_classification,
        b.payload_magneticField_anomaly,
        a.mmsi, -- MMSI [2]
        CAST(a.event_timestamp AS TIMESTAMP) AS ship_time, -- Event Timestamp [2]
        s.Name AS sanctioned_name, -- Name [3]
        s.Sanction_Reason, -- Sanktionsgrund [3]
        
        -- Berechnung der geodätischen Distanz in Metern (Impala: ST_GeodesicLengthWGS84 [4])
        -- ST_LineString verwendet hier die Doppel-Koordinaten (Lon, Lat, Lon, Lat) [5]
        ST_GeodesicLengthWGS84(
            ST_LineString(b.buoy_lon, b.buoy_lat, a.longitude, a.latitude) 
        ) AS distance_m 
        
    FROM
        BuoyDetections b
    INNER JOIN
        defense.ais_events_ice a 
        -- Zeitliche Proximität: AIS-Ereignis innerhalb von +/- 10 Minuten (600 Sekunden)
        ON ABS(UNIX_TIMESTAMP(b.buoy_time) - UNIX_TIMESTAMP(CAST(a.event_timestamp AS TIMESTAMP))) <= 600
    LEFT JOIN
        defense.sanctioned_vessels s 
        ON a.mmsi = s.MMSI 
    WHERE
        -- Räumliche Proximität: Filtern auf ${proximity_radius_km} km (Impala benötigt Meter)
        ST_GeodesicLengthWGS84(
            ST_LineString(b.buoy_lon, b.buoy_lat, a.longitude, a.latitude)
        ) <= ${proximity_radius_km} * 1000.0 
),

-- CTE 3: Korrelation mit Marine-Schiffen (Lokation und Status)
MarineCorrelations AS (
    SELECT
        b.buoyid,
        b.buoy_time,
        v.MMSI AS marine_mmsi, -- MMSI (STRING) [6]
        CAST(v.Event_Timestamp AS TIMESTAMP) AS marine_time, -- Event_Timestamp [6]
        v.Operational_Status, -- Status [6]
        
        -- Berechnung der geodätischen Distanz in Metern
        ST_GeodesicLengthWGS84(
            ST_LineString(b.buoy_lon, b.buoy_lat, v.Longitude, v.Latitude)
        ) AS distance_m 
        
    FROM
        BuoyDetections b
    INNER JOIN
        defense.marine_vessel_status v 
        -- Zeitliche Proximität: Marine-Ereignis innerhalb von +/- 10 Minuten (600 Sekunden)
        ON ABS(UNIX_TIMESTAMP(b.buoy_time) - UNIX_TIMESTAMP(CAST(v.Event_Timestamp AS TIMESTAMP))) <= 600
    WHERE
        -- Räumliche Proximität: Filtern auf ${proximity_radius_km} km
        ST_GeodesicLengthWGS84(
            ST_LineString(b.buoy_lon, b.buoy_lat, v.Longitude, v.Latitude)
        ) <= ${proximity_radius_km} * 1000.0
),

-- CTE 4: Korrelation mit Social Media (zeitlich und geographisch)
SocialMediaCorrelations AS (
    SELECT
        b.buoyid,
        b.buoy_time,
        s.tweet, -- Tweet [7]
        s.user_username, -- Nutzername [7]
        s.priority, -- Priorität [7]
        CAST(s.ts AS TIMESTAMP) AS social_time, -- Zeitstempel [7]
        
        -- Berechnung der geodätischen Distanz in Metern
        ST_GeodesicLengthWGS84(
            ST_LineString(b.buoy_lon, b.buoy_lat, s.longitude, s.latitude)
        ) AS distance_m 
        
    FROM
        BuoyDetections b
    INNER JOIN
        defense.social_media_messages s 
        -- Zeitliche Proximität: Social Media Post innerhalb von +/- 30 Minuten (1800 Sekunden)
        ON ABS(UNIX_TIMESTAMP(b.buoy_time) - UNIX_TIMESTAMP(CAST(s.ts AS TIMESTAMP))) <= 1800
    WHERE
        -- Räumliche Proximität: Filtern auf ${proximity_radius_km} km
        ST_GeodesicLengthWGS84(
            ST_LineString(b.buoy_lon, b.buoy_lat, s.longitude, s.latitude)
        ) <= ${proximity_radius_km} * 1000.0
),
-- CTE 5: Aggregation der Surveillance Reports (Lösung des Correlated Subquery Problems)
ReportCorrelations_Aggregated AS (
    SELECT
        b.buoyid,
        b.buoy_time,
        GROUP_CONCAT(
            CONCAT('ID: ', r.message_id, ' | Betreff: ', r.message_subject, ' | Quelle: ', r.message_from, ' | Position: ', r.message_position) 
            , ' ||| '
        ) AS Korrelierte_Surveillance_Reports
    FROM
        BuoyDetections b
    INNER JOIN
        defense.maritime_surveillance_reports r 
        -- Zeitliche Korrelation (Bericht DTG innerhalb von +/- 1 Stunde, 3600 Sekunden)
        ON ABS(UNIX_TIMESTAMP(b.buoy_time) - UNIX_TIMESTAMP(CAST(r.message_dtg AS TIMESTAMP))) <= 3600 
    GROUP BY
        b.buoyid, b.buoy_time
)

-- FINAL SELECT: Erstellung des umfassenden Lagebildes
SELECT
    Ais.buoyid,
    Ais.buoy_time,
    Ais.buoy_lat,
    Ais.buoy_lon,
    Ais.payload_object_classification AS Detektiertes_Objekt,
    Ais.payload_magneticField_anomaly AS Magnetische_Anomalie,
    
    -- Aggregation der AIS-Korrelation (MMSIs in der Nähe)
    GROUP_CONCAT(DISTINCT 
        CONCAT(CAST(Ais.mmsi AS STRING), 
               ' (Distanz: ', 
               -- Explizite Umwandlung des DOUBLE-Werts in STRING ist notwendig
               CAST(ROUND(Ais.distance_m / 1000.0, 2) AS STRING), 
               ' km)'
        )
    , '; ') AS MMSIs_in_Nähe,

    -- Aggregation der sanktionierten Schiffe
    GROUP_CONCAT(DISTINCT 
        CASE 
            WHEN Ais.sanctioned_name IS NOT NULL
            THEN CONCAT(Ais.sanctioned_name, ' [Grund: ', Ais.Sanction_Reason, ']') 
            ELSE NULL 
        END, ' | ') AS Sanktioniertes_Schiff_Korrelation,

    -- Aggregation der Marine-Korrelation
    GROUP_CONCAT(DISTINCT 
        CONCAT('MMSI: ', CAST(M.marine_mmsi AS STRING), 
               ' (Status: ', M.Operational_Status, ' | Distanz: ', 
               CAST(ROUND(M.distance_m / 1000.0, 2) AS STRING), 
               ' km)'
        )
    , '; ') AS Marine_Aktivität_in_Nähe,

    -- Aggregation der Social-Media-Korrelation
    GROUP_CONCAT(DISTINCT 
        CONCAT('@', S.user_username, ' [Prio: ', S.priority, ']: ', S.tweet) 
    , ' ||| ') AS Relevante_Tweets,

    -- Korrelierte Berichte (jetzt aus der separaten CTE)
    R.Korrelierte_Surveillance_Reports

FROM
    AisCorrelations Ais
LEFT JOIN
    MarineCorrelations M ON Ais.buoyid = M.buoyid AND Ais.buoy_time = M.buoy_time 
LEFT JOIN
    SocialMediaCorrelations S ON Ais.buoyid = S.buoyid AND Ais.buoy_time = S.buoy_time
LEFT JOIN
    ReportCorrelations_Aggregated R ON Ais.buoyid = R.buoyid AND Ais.buoy_time = R.buoy_time

GROUP BY 1, 2, 3, 4, 5, 6, R.Korrelierte_Surveillance_Reports -- Muss gruppiert werden, da R aggregiert wurde
ORDER BY Ais.buoy_time DESC;


