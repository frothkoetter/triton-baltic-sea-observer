Für die räumliche Analyse in **Trino** wird der Standard **OGC Well-Known Text (WKT)** verwendet. Damit Schiffsrouten mit Unterwasser-Infrastrukturen (wie der *Nord Stream* oder *Balticconnector* in der Ostsee) verschnitten werden können, werden die Leitungen als `LINESTRING` (bzw. `MULTILINESTRING`) und die AIS-Positionen als `POINT` modelliert.

Um AIS-Überquerungen zuverlässig zu erkennen, reicht ein einfacher Punkt-auf-Linie-Vergleich (`ST_Intersects(Punkt, Linie)`) meist nicht aus, da GPS-Signale in Intervallen (z. B. alle paar Sekunden bis Minuten) gesendet werden. Stattdessen verbindet man aufeinanderfolgende AIS-Positionen eines Schiffes zu einer Bewegungslinie (Trajektorie) und prüft, ob diese die Pipeline schneidet.

### **1\. Geographische Objekte (WKT-Standard) für die Ostsee**

In Trino werden geographische Koordinaten immer in der Reihenfolge **`Längengrad Breitengrad` (`Longitude Latitude`, WGS 84 / SRID 4326\)** angegeben.

* **Beispiel Pipeline/Kabel (schematischer Verlauf in der Ostsee als `LINESTRING`):**  
* Plaintext

```
LINESTRING(13.6500 54.1500, 14.8000 54.8500, 16.2000 55.4000, 19.5000 57.2000, 28.0500 59.5000)
```

*   
* **Beispiel AIS-Schiffsposition (`POINT`):**  
* Plaintext

```
POINT(15.4500 55.1200)
```

* 

### **2\. SQL-Umsetzung in Trino (Geospatial Functions)**

Trino unterscheidet zwischen dem planaren Datentyp `Geometry` und dem erdkrümmungsbasierten Datentyp `SphericalGeography` (empfohlen für reale Distanzen und lange Strecken auf See).

#### **Schritt A: Tabellen für Infrastruktur und AIS-Daten anlegen**

SQL

```
-- 1. Tabelle für Unterseekabel & Pipelines
CREATE TABLE ostsee_infrastruktur (
    id VARCHAR,
    name VARCHAR,
    typ VARCHAR, -- z.B. 'Gaspipeline' oder 'Seekabel'
    geometrie_wkt VARCHAR
);

INSERT INTO ostsee_infrastruktur VALUES
('NS1_MOCK', 'Ostsee Pipeline Route A (Beispiel)', 'Gaspipeline', 
 'LINESTRING(13.6500 54.1500, 14.8000 54.8500, 16.2000 55.4000, 19.5000 57.2000, 28.0500 59.5000)'),
('CABLE_MOCK', 'Baltic Sea Kabel B (Beispiel)', 'Seekabel', 
 'LINESTRING(14.7000 55.1000, 15.8000 55.6000, 17.9000 56.8000)');

-- 2. Beispielhafte AIS-Positionsdaten
CREATE TABLE ais_daten (
    mmsi BIGINT,
    zeitstempel TIMESTAMP,
    lon DOUBLE,
    lat DOUBLE,
    sog DOUBLE -- Speed over Ground in Knoten
);
```

#### **Schritt B: SQL-Abfrage zur Erkennung mehrfacher Überquerungen**

Diese Abfrage nutzt Window-Funktionen (`LEAD`), um jeweils zwei aufeinanderfolgende AIS-Signale eines Schiffes zu einem Fahrtsegment (`LINESTRING`) zu verbinden, und prüft mittels `ST_Intersects`, ob dieses Segment eine Pipeline kreuzt.

SQL

```
WITH schiffs_segmente AS (
    SELECT 
        mmsi,
        zeitstempel AS start_zeit,
        LEAD(zeitstempel) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_zeit,
        lon AS start_lon,
        lat AS start_lat,
        LEAD(lon) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_lon,
        LEAD(lat) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_lat
    FROM ais_daten
),
fahrt_linien AS (
    SELECT 
        mmsi,
        start_zeit,
        end_zeit,
        -- Erzeugt aus zwei aufeinanderfolgenden AIS-Punkten ein Liniensegment
        ST_LineFromText(
            'LINESTRING(' || CAST(start_lon AS VARCHAR) || ' ' || CAST(start_lat AS VARCHAR) || ', ' || 
                             CAST(end_lon AS VARCHAR) || ' ' || CAST(end_lat AS VARCHAR) || ')'
        ) AS segment_geom
    FROM schiffs_segmente
    WHERE end_lon IS NOT NULL 
      AND end_lat IS NOT NULL
      -- Filtert Ausreißer/Sprünge heraus (z.B. wenn Zeitlücke > 2 Stunden)
      AND date_diff('minute', start_zeit, end_zeit) <= 120
),
infrastruktur_geo AS (
    SELECT 
        id,
        name,
        typ,
        ST_GeometryFromText(geometrie_wkt) AS pipe_geom
    FROM ostsee_infrastruktur
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
    -- Prüft, ob das Fahrtsegment die Pipeline/das Kabel schneidet
    ON ST_Intersects(f.segment_geom, i.pipe_geom)
GROUP BY 
    f.mmsi, 
    i.name, 
    i.typ
-- Filtert auf Schiffe, die die Leitung mehrfach (öfter als 1x) überquert haben
HAVING COUNT(*) > 1
ORDER BY anzahl_ueberquerungen DESC;
```

### **3\. Wichtige Praxis-Hinweise für AIS & Geodaten**

* **WKT vs. GeoJSON:** Alternativ zu `ST_GeometryFromText(...)` unterstützt Trino auch den Import über `ST_GeomFromGeoJSON(...)`, falls reale Leitungsdaten aus Open-Data-Portalen (z. B. EMODnet oder HELCOM für die Ostsee) als `.geojson` vorliegen.  
* **Pufferzone (Korridor) statt exakter Linie:** Wenn du auch Schiffe erkennen willst, die extrem dicht an einer Pipeline anhalten, ankern oder kreuzen (ohne sie exakt zu schneiden), kannst du um die Pipeline einen Puffer legen:  
* SQL

```
-- Erstellt einen Puffer von ca. 500 Metern (0.005 Grad) um die Pipeline
ST_Buffer(i.pipe_geom, 0.005)
```

*   
* **Performance bei großen AIS-Mengen:** `ST_Intersects` berechnet für jedes Paar die genaue Geometrie. Bei Millionen von AIS-Zeilen sollte im `JOIN` vorab eine grobe Bounding-Box-Prüfung (`WHERE f.start_lon BETWEEN ...`) erfolgen, damit Trino nicht alle weltweiten Schiffspositionen gegeneinander rechnet.

Reale Open-Data-Datensätze (wie die **HELCOM Map and Data Service** oder **EMODnet Human Activities**) liegen meist als klassische FeatureCollection vor. Da Trino eine solche verschachtelte JSON-Datei nicht direkt per CREATE TABLE wie eine flache CSV-Tabelle einlesen kann, wird das GeoJSON zunächst als roher Text geladen (oder per JSON-Pfad entpackt) und anschließend mit Trinos JSON- und Geospatial-Funktionen (json\_extract, UNNEST, ST\_GeomFromGeoJSON) in echte Geometrien umgewandelt.

### **1\. Struktur eines realen GeoJSON-Exports (EMODnet / HELCOM)**

Ein typischer Export für Ostsee-Pipelines oder Seekabel sieht strukturell so aus (wobei die gesamte Datei oft in einer einzigen Zeile oder als komplettes JSON-Objekt im Object Storage wie S3/MinIO liegt):

JSON

```

{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "name": "Nord Stream 1",
        "medium": "Gas",
        "status": "In use",
        "country": "DE-RU"
      },
      "geometry": {
        "type": "MultiLineString",
        "coordinates": [
          [
            [13.6512, 54.1523],
            [14.8045, 54.8511],
            [16.2019, 55.4088]
          ]
        ]
      }
    }
  ]
}

```

### **2\. GeoJSON in Trino einbinden (S3 / Hive / Iceberg Connector)**

Damit Trino die Datei lesen kann, legst du sie in deinem Storage (z. B. S3-Bucket s3://geodaten/ostsee/pipelines.geojson) ab und erstellst eine externe Staging-Tabelle.

#### **Schritt A: Externe Tabelle für das Roh-GeoJSON anlegen**

Hierfür nutzt man ein Format, das die gesamte Datei (oder jede Zeile) als einen VARCHAR\-String einliest (z. B. TEXTFILE mit einem Trennzeichen, das im JSON nicht vorkommt, wie \\x01):

SQL

```

CREATE TABLE hive.geodaten.raw_ostsee_geojson (
    json_payload VARCHAR
)
WITH (
    external_location = 's3a://geodaten/ostsee/',
    format = 'TEXTFILE',
    textfile_field_separator = '\x01'
);

```

*(Hinweis: Falls das GeoJSON über mehrere Zeilen formatiert ist ("Pretty Printed"), muss es vor dem Upload mit einem Einzeiler wie* jq \-c . pipelines.geojson \> pipelines\_compact.geojson *in eine einzelne Zeile komprimiert werden, damit* TEXTFILE *das gesamte JSON-Dokument als eine Zeile liest.)*

### **3\. GeoJSON entpacken und als Trino-Geometrie speichern**

Mit json\_extract und CROSS JOIN UNNEST zerlegst du die FeatureCollection in einzelne Zeilen und wandelst das geometry\-Objekt direkt mit ST\_GeomFromGeoJSON in einen nativen Trino-Geometrie-Datentyp um.

SQL

```

CREATE TABLE iceberg.geodaten.ostsee_pipelines AS
WITH parsed_features AS (
    SELECT 
        -- Entpackt das Array "features" in einzelne JSON-Objekte
        CAST(json_extract(json_payload, '$.features') AS ARRAY(JSON)) AS feature_array
    FROM hive.geodaten.raw_ostsee_geojson
)
SELECT 
    -- Metadaten aus "properties" extrahieren
    json_extract_scalar(feature, '$.properties.name') AS pipeline_name,
    json_extract_scalar(feature, '$.properties.medium') AS medium,
    json_extract_scalar(feature, '$.properties.status') AS status,
    json_extract_scalar(feature, '$.geometry.type') AS geo_typ,
    
    -- Wandelt den GeoJSON-Geometrie-Block direkt in ein Trino Geometry-Objekt um
    ST_GeomFromGeoJSON(CAST(json_extract(feature, '$.geometry') AS VARCHAR)) AS geom
FROM parsed_features
CROSS JOIN UNNEST(feature_array) AS t(feature)
-- Filtert leere oder ungültige Geometrien heraus
WHERE json_extract(feature, '$.geometry') IS NOT NULL;

```

### **4\. Reale Pipeline-Geometrien mit AIS-Daten abfragen**

Sobald ostsee\_pipelines als Iceberg- oder Hive-Tabelle vorliegt, sind die Geometrien bereits geparst (GEOMETRY\-Typ). Das spart bei jeder Abfrage Rechenzeit, weil ST\_GeomFromGeoJSON nicht mehr zur Laufzeit ausgeführt werden muss.

Besonderheit bei realen Daten von EMODnet/HELCOM: Die Geometrien sind oft vom Typ MultiLineString statt LineString. Die Funktion ST\_Intersects in Trino verarbeitet jedoch sowohl LineString als auch MultiLineString problemlos.

SQL

```

WITH schiffs_segmente AS (
    SELECT 
        mmsi,
        zeitstempel AS start_zeit,
        LEAD(zeitstempel) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_zeit,
        lon AS start_lon,
        lat AS start_lat,
        LEAD(lon) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_lon,
        LEAD(lat) OVER (PARTITION BY mmsi ORDER BY zeitstempel) AS end_lat
    FROM ais_daten
),
fahrt_linien AS (
    SELECT 
        mmsi,
        start_zeit,
        end_zeit,
        ST_LineFromText(
            'LINESTRING(' || CAST(start_lon AS VARCHAR) || ' ' || CAST(start_lat AS VARCHAR) || ', ' || 
                             CAST(end_lon AS VARCHAR) || ' ' || CAST(end_lat AS VARCHAR) || ')'
        ) AS segment_geom
    FROM schiffs_segmente
    WHERE end_lon IS NOT NULL 
      AND date_diff('minute', start_zeit, end_zeit) <= 120
)
SELECT 
    f.mmsi,
    p.pipeline_name,
    p.medium,
    p.status,
    COUNT(*) AS anzahl_ueberquerungen,
    MIN(f.start_zeit) AS erste_ueberquerung,
    MAX(f.end_zeit) AS letzte_ueberquerung
FROM fahrt_linien f
JOIN iceberg.geodaten.ostsee_pipelines p
    -- Funktioniert nahtlos mit LineString und MultiLineString aus dem GeoJSON
    ON ST_Intersects(f.segment_geom, p.geom)
WHERE p.status = 'In use'
GROUP BY 
    f.mmsi, 
    p.pipeline_name, 
    p.medium, 
    p.status
HAVING COUNT(*) > 1
ORDER BY anzahl_ueberquerungen DESC;

```

### **Typische Stolpersteine bei EMODnet / HELCOM GeoJSONs**

1. **Koordinatensystem (CRS):** Achte beim Download darauf, dass **WGS 84 (EPSG:4326)** gewählt ist. Manche europäischen Geoportale bieten standardmäßig *ETRS89-LAEA (EPSG:3035)* an – diese Meter-Koordinaten passen nicht zu den Längen-/Breitengraden der AIS-Daten.  
2.   
3. **3D-Koordinaten (**Z**\-Werte):** Manche Seekabel-Datensätze enthalten Höhen-/Tiefenangaben (\[lon, lat, depth\]). ST\_GeomFromGeoJSON in Trino ignoriert die dritte Koordinate automatisch und verarbeitet sauber die 2D-Projektion (lon, lat).  
4.   
5. **Performance-Boost durch Bounding Box:** Da eine Pipeline quer durch die Ostsee ein riesiges umgebendes Rechteck aufspannt, empfiehlt es sich bei großen Tabellen, lange Pipelines vorab mit ST\_GeometryN oder einer Segmentierung in kleinere Teilstücke zu zerlegen, damit Spatial-Joins deutlich schneller laufen.

Eine einzelne Gaspipeline wie *Nord Stream* spannt eine Bounding Box über fast die gesamte Ostsee auf. Bei einem Spatial-Join prüft Trino zunächst die Bounding Boxen: Überlappt das Rechteck der Pipeline mit dem Schiff, wird die teure exakte Geometrie berechnet. Durch das Zerlegen der MultiLineString\-Geometrien in kleine Einzelabschnitte (Sub-Segmente) schrumpfen diese Bounding Boxen dramatisch, was die Abfrage oft um das **10- bis 50-fache beschleunigt**.

Gleichzeitig lässt sich durch einen Puffer-Korridor (ST\_Buffer) und die Auswertung der Schiffsgeschwindigkeit (SOG – Speed Over Ground) erkennen, ob ein Schiff nicht nur normal kreuzt, sondern verdächtig langsam fährt, treibt oder ankert.

### **1\. Performance-Optimierung: Pipelines in kurze Segmente zerlegen**

Dieser Schritt wird **einmalig beim Laden** der Pipeline-Daten ausgeführt. Ein langer MultiLineString wird zuerst in einzelne LineStrings (ST\_GeometryN) und anschließend in 2-Punkt-Abschnitte (ST\_PointN) zerlegt. Für jeden Abschnitt speichern wir zusätzlich die Bounding Box (min\_lon, max\_lon, min\_lat, max\_lat) für einen schnellen Vorfilter.

SQL

```

CREATE TABLE iceberg.geodaten.ostsee_pipelines_segmented AS
WITH single_lines AS (
    -- 1. MultiLineStrings in einzelne LineStrings aufteilen
    SELECT 
        pipeline_name,
        medium,
        status,
        ST_GeometryN(geom, idx) AS line_geom
    FROM iceberg.geodaten.ostsee_pipelines
    CROSS JOIN UNNEST(sequence(1, ST_NumGeometries(geom))) AS t(idx)
),
line_segments AS (
    -- 2. Jeden LineString in einzelne 2-Punkt-Segmente zerlegen
    SELECT 
        pipeline_name,
        medium,
        status,
        ST_LineFromText(
            'LINESTRING(' || 
            CAST(ST_X(ST_PointN(line_geom, pt_idx)) AS VARCHAR) || ' ' || 
            CAST(ST_Y(ST_PointN(line_geom, pt_idx)) AS VARCHAR) || ', ' ||
            CAST(ST_X(ST_PointN(line_geom, pt_idx + 1)) AS VARCHAR) || ' ' || 
            CAST(ST_Y(ST_PointN(line_geom, pt_idx + 1)) AS VARCHAR) || ')'
        ) AS seg_geom
    FROM single_lines
    CROSS JOIN UNNEST(sequence(1, ST_NumPoints(line_geom) - 1)) AS t(pt_idx)
)
SELECT 
    pipeline_name,
    medium,
    status,
    seg_geom,
    -- 500-Meter-Korridor vorberechnen (ca. 0.005 Grad auf Höhe der Ostsee)
    ST_Buffer(seg_geom, 0.005) AS buffer_500m_geom,
    -- Bounding Box inklusive Puffer für extrem schnelle Joins
    ST_XMin(ST_Buffer(seg_geom, 0.005)) AS min_lon,
    ST_XMax(ST_Buffer(seg_geom, 0.005)) AS max_lon,
    ST_YMin(ST_Buffer(seg_geom, 0.005)) AS min_lat,
    ST_YMax(ST_Buffer(seg_geom, 0.005)) AS max_lat
FROM line_segments;

```

### **2\. Kombinierte Abfrage: Mehrfach-Überquerungen & Langsamfahrt/Ankern**

Diese Abfrage verknüpft die optimierte Segment-Tabelle mit den AIS-Daten und prüft zwei Muster gleichzeitig:

1. **Direkte Überquerung:** Das Fahrtsegment des Schiffes schneidet die Leitung (ST\_Intersects(f.segment\_geom, p.seg\_geom)).  
2.   
3. **Verdächtiges Verhalten im 500m-Korridor:** Das Schiff befindet sich innerhalb der Schutzzone (buffer\_500m\_geom) und fährt langsamer als **3 Knoten** (SOG \< 3.0), was auf Ankern, Treiben oder Unterwasser-Aktivitäten hindeutet.  
4. 

SQL

```

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
    FROM ais_daten
),
fahrt_linien AS (
    SELECT 
        mmsi,
        start_zeit,
        end_zeit,
        date_diff('second', start_zeit, end_zeit) / 60.0 AS dauer_minuten,
        -- Durchschnittsgeschwindigkeit des Segments
        (start_sog + COALESCE(end_sog, start_sog)) / 2.0 AS avg_sog,
        LEAST(start_lon, end_lon) AS ship_min_lon,
        GREATEST(start_lon, end_lon) AS ship_max_lon,
        LEAST(start_lat, end_lat) AS ship_min_lat,
        GREATEST(start_lat, end_lat) AS ship_max_lat,
        ST_LineFromText(
            'LINESTRING(' || CAST(start_lon AS VARCHAR) || ' ' || CAST(start_lat AS VARCHAR) || ', ' || 
                             CAST(end_lon AS VARCHAR) || ' ' || CAST(end_lat AS VARCHAR) || ')'
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
        -- Prüft exakte Überquerung der Pipeline
        MAX(CASE WHEN ST_Intersects(f.segment_geom, p.seg_geom) THEN 1 ELSE 0 END) AS ist_ueberquerung,
        -- Prüft Langsamfahrt / Ankern im 500m-Korridor
        MAX(CASE WHEN f.avg_sog < 3.0 THEN 1 ELSE 0 END) AS ist_langsamfahrt
    FROM fahrt_linien f
    JOIN iceberg.geodaten.ostsee_pipelines_segmented p
        -- 1. Extrem schneller Bounding-Box-Vorfilter (nutzt Min/Max-Statistiken in Iceberg/Parquet)
        ON f.ship_max_lon >= p.min_lon 
       AND f.ship_min_lon <= p.max_lon
       AND f.ship_max_lat >= p.min_lat 
       AND f.ship_min_lat <= p.max_lat
       -- 2. Exakte Prüfung gegen den 500-Meter-Korridor
       AND ST_Intersects(f.segment_geom, p.buffer_500m_geom)
    WHERE p.status = 'In use'
    -- Gruppiert pro Schiffssegment, damit ein langes Schiffsegment nicht doppelt gezählt wird,
    -- falls es zwei aneinanderstoßende Pipeline-Segmente gleichzeitig berührt
    GROUP BY 
        f.mmsi, p.pipeline_name, p.medium, f.start_zeit, f.end_zeit, f.dauer_minuten, f.avg_sog
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
GROUP BY 
    mmsi, 
    pipeline_name, 
    medium
-- Filter: Mehrfache Überquerungen ODER verdächtige Langsamfahrt (> 15 Min. unter 3 Knoten im Korridor)
HAVING SUM(ist_ueberquerung) > 1 
    OR SUM(CASE WHEN ist_langsamfahrt = 1 THEN dauer_minuten ELSE 0 END) > 15
ORDER BY 
    minuten_langsam_im_korridor DESC, 
    anzahl_exakte_ueberquerungen DESC;

```

### **Warum diese Architektur in der Praxis entscheidend ist**

* **Vermeidung von Doppelzählungen:** Wenn ein 500-Meter-Puffer um jedes 2-Punkt-Segment der Pipeline gelegt wird, überlappen sich die Puffer an den Knickpunkten leicht. Durch das innere GROUP BY auf das Schiffssegment (f.start\_zeit, f.end\_zeit) mit MAX(CASE WHEN ...) wird ein einzelnes AIS-Fahrtsegment niemals mehrfach gezählt, selbst wenn es drei Pipeline-Teilstücke gleichzeitig berührt.  
*   
* **Präzision von** ST\_Buffer **in der Ostsee:** Auf 55° bis 60° nördlicher Breite (Ostsee) entspricht 1° Breite ca. 111 km, aber 1° Länge nur noch ca. 55–63 km. Ein planarer Puffer von 0.005° ergibt dort eine leicht ovale Schutzzone von ca. **550 Metern in Nord-Süd-Richtung** und **300 Metern in Ost-West-Richtung**. Falls metergenaue Radien gefordert sind, lässt sich im letzten Schritt ST\_Distance(to\_spherical\_geography(punkt1), to\_spherical\_geography(punkt2)) für die exakte Meter-Distanz ergänzen.  
*   
* **AIS-Lücken (Dark Activity):** Ein besonders kritisches Muster bei Pipeline-Vorfällen ist das Abschalten des AIS-Transponders unmittelbar vor der Leitung. Das obige WHERE date\_diff('minute', start\_zeit, end\_zeit) \<= 120 filtert lange Lücken bewusst heraus, damit keine falschen Luftlinien über halbe Länder gezogen werden – genau diese Zeitlücken (\> 30 Minuten) in Kombination mit ST\_Intersects lassen sich aber in einer separaten Abfrage nutzen, um **AIS-Blackouts direkt über Pipelines** aufzuspüren.

Reale, kostenfrei nutzbare Geodaten (Open Data) für Ostsee-Pipelines und Unterseekabel lassen sich direkt über offizielle europäische Meeresdaten-Portale herunterladen oder automatisiert per WFS-Schnittstelle (Web Feature Service) als GeoJSON abrufen.

### **1\. Die besten Open-Data-Quellen für die Ostsee**

| Quelle | Abdeckung & Fokus | Verfügbare Formate |
| :---- | :---- | :---- |
| **EMODnet Human Activities** | Gesamte europäische Meere (inkl. Ostsee); sehr detaillierte Attribute (Medium, Status, Betreiber, Durchmesser) für Pipelines & Seekabel. | GeoJSON, Shapefile, GeoPackage, WFS |
| **HELCOM Map and Data Service** | Speziell auf den Ostseeraum (Baltic Sea) zugeschnitten; umfasst *Cables* und *Pipelines*. | GeoJSON, Shapefile, WFS |
| **BSH GeoSeaPortal** | Bundesamt für Seeschifffahrt und Hydrographie; sehr präzise Daten für die deutsche AWZ in Nord- und Ostsee. | WMS, WFS, Shapefile |
| **OpenStreetMap (Overpass API)** | Weltweit; gute Ergänzung für küstennahe Trassen und Anlandungspunkte (man\_made=pipeline, submarine=yes). | GeoJSON, OSM XML |

### **2\. Direkter Download per WFS-API (GeoJSON)**

Statt sich durch Web-Karten zu klicken, kannst du die GeoJSON-Dateien direkt per curl oder Python über die **OGC WFS-Schnittstellen** von EMODnet und HELCOM herunterladen – inklusive räumlichem Filter (Bounding Box für die Ostsee: 9.0, 53.5, 30.5, 66.0).

#### **Option A: EMODnet Pipelines (Europa / Ostsee)**

Dieser Befehl lädt alle Pipelines im Ostsee-Rechteck direkt im korrekten Koordinatensystem **WGS 84 (**EPSG:4326**)** als kompaktes GeoJSON herunter:

Bash

```

curl -G "https://ows.emodnet-humanactivities.eu/wfs" \
  --data-urlencode "service=WFS" \
  --data-urlencode "version=2.0.0" \
  --data-urlencode "request=GetFeature" \
  --data-urlencode "typeName=emodnet:pipelines" \
  --data-urlencode "outputFormat=application/json" \
  --data-urlencode "srsName=EPSG:4326" \
  --data-urlencode "bbox=9.0,53.5,30.5,66.0,EPSG:4326" \
  -o ostsee_pipelines_emodnet.geojson

```

#### **Option B: EMODnet Unterseekabel (Telekommunikation & Strom)**

Für Seekabel (wie *C-Lion1*, *NordBalt* oder *Baltic Cable*) wird lediglich der typeName angepasst:

Bash

```

curl -G "https://ows.emodnet-humanactivities.eu/wfs" \
  --data-urlencode "service=WFS" \
  --data-urlencode "version=2.0.0" \
  --data-urlencode "request=GetFeature" \
  --data-urlencode "typeName=emodnet:cables" \
  --data-urlencode "outputFormat=application/json" \
  --data-urlencode "srsName=EPSG:4326" \
  --data-urlencode "bbox=9.0,53.5,30.5,66.0,EPSG:4326" \
  -o ostsee_cables_emodnet.geojson

```

#### **Option C: HELCOM Ostsee-Datensatz (Pipelines & Kabel)**

HELCOM stellt die Ostsee-Daten über ArcGIS-REST- bzw. WFS-Endpunkte bereit. Über den Web-Katalog (**maps.helcom.fi/website/mapservice**) kannst du unter *Pressures and Human Activities \-\> Sea-use \-\> Cables / Pipelines* direkt auf **Download** klicken und *GeoJSON* auswählen.

### **3\. Vorbereitung für Trino (Einzeiler-Konvertierung)**

WFS-Server liefern das GeoJSON oft mehrzeilig oder mit einer Bounding-Box-Metadatenzeile aus. Damit dein Trino TEXTFILE\-Import (raw\_ostsee\_geojson) die komplette FeatureCollection als exakt **eine Zeile** einliest, komprimierst du die Datei vor dem Upload nach S3 mit jq:

Bash

```

# Komprimiert das GeoJSON auf eine einzige Zeile ohne Zeilenumbrüche
jq -c . ostsee_pipelines_emodnet.geojson > ostsee_pipelines_trino.geojson

# Upload in deinen S3-Bucket für Trino
aws s3 cp ostsee_pipelines_trino.geojson s3://geodaten/ostsee/

```

### **4\. Wichtige Feldnamen im EMODnet-GeoJSON anpassen**

Je nach Quelle heißen die Attribute unter properties leicht unterschiedlich. Bei **EMODnet (**emodnet:pipelines**)** lauten die wichtigsten Felder für dein Trino CREATE TABLE\-Skript:

* \$.properties.name (Name der Pipeline, z. B. *Nord Stream 1*, *Balticconnector*)  
*   
* \$.properties.medium (z. B. *Gas*, *Oil*)  
*   
* \$.properties.status (z. B. *In use*, *Decommissioned*, *Under construction*)  
*   
* \$.properties.operator (Betreibergesellschaft)  
*   
* \$.properties.country (Ländercode)

Dieses Python-Skript ruft die aktuellen **EMODnet Human Activities** WFS-Endpunkte für Pipelines und Seekabel in der Ostsee ab, bereinigt die Geometrien (z. B. Entfernung von 3D-Z-Koordinaten, Filterung ungültiger Einträge) und bietet dir zwei optimierte Ausgabeformate für **S3 / Trino**:

1. **Parquet mit WKT-Spalte (Empfohlen für Produktion):** Spart das aufwendige JSON-Parsing in Trino und kann direkt als externe Hive- oder Iceberg-Tabelle eingebunden werden.  
2.   
3. **Kompaktes Einzeiler-GeoJSON:** Kompatibel mit dem TEXTFILE\-Import-Workflow aus den vorherigen Schritten.  
4. 

### **1\. Voraussetzungen installieren**

Für die Verarbeitung der Geodaten und den direkten S3-Upload werden requests, shapely, pyarrow und boto3 benötigt:

Bash

```

pip install requests shapely pyarrow boto3

```

### **2\. Python-Skript:** sync\_emodnet\_baltic.py

Python

```

import io
import json
import logging
from datetime import datetime, timezone
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from shapely.geometry import shape
from shapely.ops import transform

# Logging konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Konfiguration für Ostsee-Bounding-Box (MinLon, MinLat, MaxLon, MaxLat)
BALTIC_BBOX = "9.0,53.5,30.5,66.0,EPSG:4326"
EMODNET_WFS_URL = "https://ows.emodnet-humanactivities.eu/wfs"

# S3 Ziel-Konfiguration (für MinIO endpoint_url im boto3.client ergänzen)
S3_BUCKET = "geodaten"
S3_PREFIX = "ostsee/infrastruktur"

LAYERS = {
    "pipelines": "emodnet:pipelines",
    "cables": "emodnet:cables",
}


def fetch_wfs_geojson(type_name: str, bbox: str) -> dict:
    """Lädt Features als GeoJSON (WGS 84) vom EMODnet WFS-Server."""
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": type_name,
        "outputFormat": "application/json",
        "srsName": "EPSG:4326",
        "bbox": bbox,
    }
    logging.info(f"Starte WFS-Download für Layer '{type_name}'...")
    response = requests.get(EMODNET_WFS_URL, params=params, timeout=120)
    response.raise_for_status()
    data = response.json()
    logging.info(f"Erfolgreich geladen: {len(data.get('features', []))} Features.")
    return data


def force_2d(geom):
    """Entfernt eventuelle Z-Koordinaten (Tiefe/Höhe) für sauberes 2D-WKT."""
    if geom.has_z:
        return transform(lambda x, y, z=None: (x, y), geom)
    return geom


def normalize_features(geojson_data: dict, infra_category: str) -> list[dict]:
    """Bereinigt Geometrien und vereinheitlicht die Attribute für Pipelines & Kabel."""
    cleaned_records = []
    download_ts = datetime.now(timezone.utc).isoformat()

    for feature in geojson_data.get("features", []):
        raw_geom = feature.get("geometry")
        props = feature.get("properties") or {}

        if not raw_geom:
            continue

        try:
            geom_obj = shape(raw_geom)
            if geom_obj.is_empty:
                continue

            # In 2D umwandeln & einfache topologische Fehler reparieren
            geom_2d = force_2d(geom_obj)
            if not geom_2d.is_valid:
                geom_2d = geom_2d.buffer(0)

            minx, miny, maxx, maxy = geom_2d.bounds

            # EMODnet nutzt bei Kabeln und Pipelines teils leicht abweichende Property-Namen
            name = (
                props.get("name")
                or props.get("cable_name")
                or props.get("pipeline_name")
                or "Unbekannt"
            )
            medium = (
                props.get("medium")
                or props.get("cable_type")
                or ("Telecom/Power" if infra_category == "cables" else "Unbekannt")
            )
            status = props.get("status") or "Unknown"
            operator = props.get("operator") or props.get("owner") or ""
            country = props.get("country") or ""

            cleaned_records.append({
                "id": str(feature.get("id", "")),
                "kategorie": infra_category,
                "name": str(name).strip(),
                "medium": str(medium).strip(),
                "status": str(status).strip(),
                "operator": str(operator).strip(),
                "country": str(country).strip(),
                "geom_type": geom_2d.geom_type,
                "wkt": geom_2d.wkt,
                "min_lon": float(minx),
                "min_lat": float(miny),
                "max_lon": float(maxx),
                "max_lat": float(maxy),
                "updated_at": download_ts,
            })
        except Exception as exc:
            logging.warning(f"Feature übersprungen (ID: {feature.get('id')}): {exc}")

    return cleaned_records


def upload_parquet_to_s3(records: list[dict], s3_client, bucket: str, key: str):
    """Schreibt die bereinigten Datensätze als komprimierte Parquet-Datei nach S3."""
    schema = pa.schema([
        ("id", pa.string()),
        ("kategorie", pa.string()),
        ("name", pa.string()),
        ("medium", pa.string()),
        ("status", pa.string()),
        ("operator", pa.string()),
        ("country", pa.string()),
        ("geom_type", pa.string()),
        ("wkt", pa.string()),
        ("min_lon", pa.float64()),
        ("min_lat", pa.float64()),
        ("max_lon", pa.float64()),
        ("max_lat", pa.float64()),
        ("updated_at", pa.string()),
    ])

    table = pa.Table.from_pylist(records, schema=schema)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    logging.info(f"Parquet hochgeladen: s3://{bucket}/{key} ({len(records)} Zeilen)")


def upload_singleline_geojson_to_s3(records: list[dict], s3_client, bucket: str, key: str):
    """Erzeugt ein kompaktes Einzeiler-GeoJSON (ohne Zeilenumbrüche) für Trino TEXTFILE."""
    from shapely import wkt as shapely_wkt
    from shapely.geometry import mapping

    features = []
    for r in records:
        geom = shapely_wkt.loads(r["wkt"])
        features.append({
            "type": "Feature",
            "id": r["id"],
            "properties": {
                "kategorie": r["kategorie"],
                "name": r["name"],
                "medium": r["medium"],
                "status": r["status"],
                "operator": r["operator"],
                "country": r["country"],
            },
            "geometry": mapping(geom),
        })

    feature_collection = {
        "type": "FeatureCollection",
        "features": features,
    }

    # separators=(",", ":") entfernt unnötige Leerzeichen und Zeilenumbrüche
    compact_json = json.dumps(feature_collection, separators=(",", ":"), ensure_ascii=False)

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=compact_json.encode("utf-8"),
        ContentType="application/geo+json",
    )
    logging.info(f"Einzeiler-GeoJSON hochgeladen: s3://{bucket}/{key}")


def main():
    # Hinweis: Bei lokalem MinIO/Ceph hier endpoint_url="http://localhost:9000" ergänzen
    s3_client = boto3.client("s3")

    all_records = []
    for category, layer_name in LAYERS.items():
        raw_geojson = fetch_wfs_geojson(layer_name, BALTIC_BBOX)
        normalized = normalize_features(raw_geojson, category)
        logging.info(f"Bereinigt für '{category}': {len(normalized)} gültige Geometrien.")
        all_records.extend(normalized)

    if not all_records:
        logging.error("Keine Datensätze gefunden. Abbruch.")
        return

    # 1. Upload als Parquet (Optimal für Trino)
    upload_parquet_to_s3(
        records=all_records,
        s3_client=s3_client,
        bucket=S3_BUCKET,
        key=f"{S3_PREFIX}/parquet/ostsee_infrastruktur.parquet",
    )

    # 2. Optionaler Upload als Einzeiler-GeoJSON
    upload_singleline_geojson_to_s3(
        records=all_records,
        s3_client=s3_client,
        bucket=S3_BUCKET,
        key=f"{S3_PREFIX}/geojson/ostsee_infrastruktur.geojson",
    )


if __name__ == "__main__":
    main()

```

### **3\. Direkte Einbindung der Parquet-Datei in Trino**

Durch die wkt\-Spalte und die vorberechneten min\_lon/max\_lon\-Bounding-Boxen entfällt das komplette JSON-Entpacken in Trino. Du kannst die Datei direkt als externe Tabelle anbinden:

SQL

```

CREATE TABLE hive.geodaten.ostsee_infrastruktur_raw (
    id VARCHAR,
    kategorie VARCHAR,
    name VARCHAR,
    medium VARCHAR,
    status VARCHAR,
    operator VARCHAR,
    country VARCHAR,
    geom_type VARCHAR,
    wkt VARCHAR,
    min_lon DOUBLE,
    min_lat DOUBLE,
    max_lon DOUBLE,
    max_lat DOUBLE,
    updated_at VARCHAR
)
WITH (
    external_location = 's3a://geodaten/ostsee/infrastruktur/parquet/',
    format = 'PARQUET'
);

```

Danach kannst du ST\_GeometryFromText(wkt) direkt nutzen, um daraus die segmentierte Iceberg-Tabelle (ostsee\_pipelines\_segmented) für deine AIS-Überquerungs- und Korridor-Analysen zu bauen.

Um verdächtige **AIS-Blackouts (Dark Activity)** über kritischer Infrastruktur zu erkennen, sucht man gezielt nach Zeitlücken zwischen zwei aufeinanderfolgenden AIS-Signalen eines Schiffes (z. B. **30 Minuten bis 24 Stunden**), bei denen das Schiff unmittelbar vor dem Signalverlust oder direkt beim Wiederauftauchen in der Nähe einer Pipeline war – oder die interpolierte Luftlinie zwischen dem letzten und dem nächsten Signal die Leitung schneidet.

Gleichzeitig muss das Skript **echte Transponder-Abschaltungen von normalen Hafenaufenthalten oder Küstenschatten** unterscheiden, da Schiffe im Hafen ihr AIS häufig regulär abschalten.

### **1\. Logik der Blackout-Erkennung in Trino**

Die Abfrage kombiniert vier Indikatoren zu einem verdächtigen Muster:

1. **Signallücke (**gap\_minuten**):** Zwischen zwei empfangenen AIS-Positionen vergehen z. B. zwischen **30 Minuten und 1.440 Minuten (24 Stunden)**.  
2.   
3. **Räumlicher Bezug zur Pipeline:** Mindestens eine der beiden Positionen (Startpunkt vor dem Blackout oder Endpunkt nach dem Blackout) liegt innerhalb eines **2-km-Korridors (**0.02°**)** um die Leitung – *oder* die direkte Verbindungslinie kreuzt die Pipeline.  
4.   
5. **Zeit-Distanz-Anomalie (Loitering-Check):** Vergleicht die tatsächlich zurückgelegte Luftlinie (distanz\_sm) mit der Strecke, die das Schiff bei seiner letzten bekannten Geschwindigkeit (start\_sog) hätte zurücklegen müssen. Wenn ein Schiff mit 12 Knoten fährt, 3 Stunden verschwindet, aber nur 2 Seemeilen weiter wieder auftaucht, hat es während des Blackouts gestoppt oder geankert.  
6.   
7. **Ausschluss von Hafenbereichen:** Filtert Ereignisse heraus, bei denen die Geschwindigkeit vor und nach der Lücke nahe 0 Knoten (\< 0.5 kn) direkt in Küsten-/Hafennähe liegt (optional über eine Hafen-Polygon-Tabelle erweiterbar).  
8. 

### **2\. Trino SQL-Skript für AIS-Blackouts über Pipelines**

Dieses Skript baut direkt auf der bereits angelegten, segmentierten Tabelle iceberg.geodaten.ostsee\_pipelines\_segmented auf:

SQL

```

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
    FROM ais_daten
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
        -- Exakte Distanz auf der Erdkugel in Seemeilen (1 sm = 1852 Meter)
        ST_Distance(
            to_spherical_geography(ST_Point(start_lon, start_lat)),
            to_spherical_geography(ST_Point(end_lon, end_lat))
        ) / 1852.0 AS distanz_sm,
        -- Erwartete Distanz bei konstanter Fahrt (SOG * Stunden)
        (COALESCE(start_sog, 0) * (date_diff('minute', blackout_start, blackout_ende) / 60.0)) AS erwartete_distanz_sm,
        -- Geometrien für den Spatial Join
        ST_Point(start_lon, start_lat) AS start_geom,
        ST_Point(end_lon, end_lat) AS end_geom,
        ST_LineFromText(
            'LINESTRING(' || CAST(start_lon AS VARCHAR) || ' ' || CAST(start_lat AS VARCHAR) || ', ' || 
                             CAST(end_lon AS VARCHAR) || ' ' || CAST(end_lat AS VARCHAR) || ')'
        ) AS luecken_linie,
        -- Bounding Box der Lücke inklusive ~2 km Suchradius (0.02 Grad)
        LEAST(start_lon, end_lon) - 0.02 AS box_min_lon,
        GREATEST(start_lon, end_lon) + 0.02 AS box_max_lon,
        LEAST(start_lat, end_lat) - 0.02 AS box_min_lat,
        GREATEST(start_lat, end_lat) + 0.02 AS box_max_lat
    FROM ais_gaps
    WHERE end_lon IS NOT NULL
      -- Fokus auf Lücken zwischen 30 Minuten und 24 Stunden
      AND date_diff('minute', blackout_start, blackout_ende) BETWEEN 30 AND 1440
      -- Schließt klassische Hafenlieger aus (Schiff war vor UND nach der Lücke fast stationär)
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
        -- Klassifizierung des Blackout-Musters
        MAX(CASE 
            WHEN ST_Distance(to_spherical_geography(v.start_geom), to_spherical_geography(p.seg_geom)) <= 2000 
            THEN 1 ELSE 0 
        END) AS start_nah_an_pipeline,
        MAX(CASE 
            WHEN ST_Distance(to_spherical_geography(v.end_geom), to_spherical_geography(p.seg_geom)) <= 2000 
            THEN 1 ELSE 0 
        END) AS ende_nah_an_pipeline,
        MAX(CASE 
            WHEN ST_Intersects(v.luecken_linie, p.seg_geom) 
            THEN 1 ELSE 0 
        END) AS kreuzt_waehrend_blackout
    FROM verdaechtige_luecken v
    JOIN iceberg.geodaten.ostsee_pipelines_segmented p
        -- 1. Schneller Bounding-Box-Vorfilter
        ON v.box_max_lon >= p.min_lon
       AND v.box_min_lon <= p.max_lon
       AND v.box_max_lat >= p.min_lat
       AND v.box_min_lat <= p.max_lat
       -- 2. Räumliche Relevanzprüfung (Start/Ende im 2km-Radius ODER Überquerung während Blackout)
       AND (
           ST_Intersects(v.luecken_linie, p.seg_geom)
           OR ST_Distance(to_spherical_geography(v.start_geom), to_spherical_geography(p.seg_geom)) <= 2000
           OR ST_Distance(to_spherical_geography(v.end_geom), to_spherical_geography(p.seg_geom)) <= 2000
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
    -- Verhältnis: Werte nahe 0 bedeuten, das Schiff hat während des Blackouts gestoppt/geankert
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
    -- Priorisiert Schiffe, die direkt an der Leitung abschalten und kaum Strecke zurücklegen
    (start_nah_an_pipeline + ende_nah_an_pipeline) DESC,
    luecke_minuten DESC;

```

### **3\. Interpretation der Ergebnisse & Feinjustierung**

* distanz\_verhaeltnis **(Loitering-Indikator):** Ein Wert unter 0.25 bei einer vorherigen Fahrtgeschwindigkeit (sog\_vor\_blackout\_kn \> 5.0) ist ein starkes Indiz dafür, dass ein Schiff in voller Fahrt das AIS abgeschaltet, an der Pipeline gestoppt oder langsam gekreuzt hat und danach erst wieder auf Sendung ging.  
*   
* **Empfangslöcher vs. echte Abschaltung:** In der offenen Ostsee (z. B. zwischen Bornholm, Gotland und dem Baltikum) können terrestrische Küstenempfänger manchmal Lücken aufweisen. Wenn deine AIS-Datenquelle ausschließlich landbasierte Empfänger nutzt, empfiehlt es sich, luecke\_minuten auf \>= 60 zu setzen oder zusätzlich auf **Satelliten-AIS (S-AIS)** zu prüfen, um Empfangsschatten auszuschließen.  
*   
* **Mehrfachtäter identifizieren:** Umschließt man die finale SELECT\-Abfrage mit einem GROUP BY mmsi, pipeline\_name HAVING COUNT(\*) \> 1, lassen sich Schiffe herausfiltern, die dieses Abschaltmuster wiederholt an derselben Infrastruktur zeigen.

