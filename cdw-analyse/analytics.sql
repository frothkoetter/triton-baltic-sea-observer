create database defense;

drop table if exists ais_events_ice;
CREATE EXTERNAL TABLE ais_events_ice(
  `mmsi` bigint,
  `event_timestamp` string, 
  `latitude` double, 
  `longitude` double,
  `speed` double, 
  `course` double, 
  `status` string,
  `Destination` string)
STORED BY 
  Iceberg;


select count(1), mmsi, destination FROM ais_events_ice 
group by destination, mmsi;


drop table if exists observation_areas; 
CREATE TABLE observation_areas (
    area_id STRING,
    polygon STRING
)
STORED AS ORC;


INSERT INTO observation_areas VALUES
('area_1', 'POLYGON((17.237478356677286 56.03107993705109, 17.237478356677286 55.9047066587936, 18.31712997618766 55.9047066587936, 18.31712997618766 56.03107993705109, 17.237478356677286 56.03107993705109))'),
('area_2', 'POLYGON((18.29518008728172 55.53540464392404, 18.29518008728172 55.22756184778888, 19.28762566297803 55.22756184778888, 19.28762566297803 55.53540464392404, 18.29518008728172 55.53540464392404))'),
('area_3', 'POLYGON((19.101293326297395 56.70066121963845, 19.101293326297395 56.60288332018746, 20.332383399655697 56.60288332018746, 20.332383399655697 56.70066121963845, 19.101293326297395 56.70066121963845))'),
('area_4', 'POLYGON((18.631009957831964 56.33596162852314, 18.631009957831964 55.68428205974104, 18.81138185878598 55.68428205974104, 18.81138185878598 56.33596162852314, 18.631009957831964 56.33596162852314))'),
('area_5', 'POLYGON((17.20764611432847 56.797018539001215, 17.20764611432847 56.27911433081525, 17.4428839762831 56.27911433081525, 17.4428839762831 56.797018539001215, 17.20764611432847 56.797018539001215))');

WITH observation_polygons AS (
    SELECT 
        area_id,
        ST_GeomFromText(polygon) AS geom
    FROM observation_areas
),
ship_positions AS (
    SELECT 
        e.mmsi,
        e.event_timestamp,
        e.latitude,
        e.longitude,
        o.area_id
    FROM 
        ais_events_ice e
    JOIN 
        observation_polygons o
    ON 
        ST_Within(ST_Point(e.longitude, e.latitude), o.geom)
),
ship_transitions AS (
    SELECT
        mmsi,
        area_id,
        event_timestamp,
        CASE 
            WHEN LAG(area_id) OVER (PARTITION BY mmsi ORDER BY event_timestamp) IS NULL 
                 OR LAG(area_id) OVER (PARTITION BY mmsi ORDER BY event_timestamp) != area_id 
            THEN 'enter'
            ELSE 'exit'
        END AS transition
    FROM 
        ship_positions
) select * from ship_transitions;


CREATE TABLE sanctioned_vessels (
    Name STRING,                     -- Name of the vessel
    MMSI BIGINT,                    -- Maritime Mobile Service Identity
    IMO BIGINT,                     -- International Maritime Organization number
    Type STRING,                    -- Type of vessel (e.g., Crude Oil Tanker, Products Tanker)
    Flag STRING,                    -- Country flag of the vessel
    Sanction_Reason STRING,         -- Reason for sanction
    Linked_To STRING                -- Entities linked to the vessel
);

INSERT INTO sanctioned_vessels (Name, MMSI, IMO, Type, Flag, Sanction_Reason, Linked_To)
VALUES
('ARISTO', 636022549, 9327413, 'Chemical/Products Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('HAI II', 636016693, 9259599, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('HS ARGE', 636022360, 9299745, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('HS ATLANTICA', 636022401, 9322839, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited; HS Atlantica Limited'),
('HS BURAQ', 636022364, 9381732, 'Products Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('HS ESBERG', 636022386, 9410894, 'Products Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('HS EVERETT', 636022403, 9410870, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('HS GLORY', 636018127, 9249087, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('HS LEGEND', 636022362, 9381744, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('HS STAR', 636018885, 9274446, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('LA PRIDE', 636022251, 9274616, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('MONA', 636022424, 9314818, 'Chemical/Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('NELLIS', 636022550, 9322267, 'Chemical/Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('OSPEROUS', 636022098, 9412995, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('PERIA', 636022479, 9322827, 'Crude Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('SARA II', 636022546, 9301615, 'Chemical/Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('SENSUS', 636022146, 9296585, 'Products Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited'),
('UZE', 636022072, 9323338, 'Chemical/Oil Tanker', 'Liberia', 'Blocked under E.O. 14024', 'Hennesea Holdings Limited');



INSERT INTO sanctioned_vessels (Name, IMO, Type, Flag, Sanction_Reason, Linked_To) VALUES
('M/V Angara', 9179842, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe a: Beförderung von im Verteidigungs- und Sicherheitssektor verwendeten Gütern und Technologien von oder nach Russland, zur Verwendung in Russland oder für die Kriegsführung Russlands in der Ukraine', 'EU'),
('M/V Maria', 8517839, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe a: Beförderung von im Verteidigungs- und Sicherheitssektor verwendeten Gütern und Technologien von oder nach Russland, zur Verwendung in Russland oder für die Kriegsführung Russlands in der Ukraine', 'EU'),
('Saam FSU', 9915090, 'Floating Storage Unit', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe c: so betrieben werden, dass sie zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beitragen oder diese unterstützen.', 'EU'),
('Koryak FSU', 9915105, 'Floating Storage Unit', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe c: so betrieben werden, dass sie zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beitragen oder diese unterstützen.', 'EU'),
('Hana', 9353113, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe b: Beförderung von Rohöl oder Erdölerzeugnissen, die in Anhang XIII aufgeführt sind, ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, sowie Anwendung irregulärer und mit hohem Risiko behafteter Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation.', 'EU'),
('Canis Power', 9289520, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe b: Beförderung von Rohöl oder Erdölerzeugnissen, die in Anhang XIII aufgeführt sind, ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, sowie Anwendung irregulärer und mit hohem Risiko behafteter Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation', 'EU'),
('Andromeda Star', 9402471, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe b: Beförderung von Rohöl oder Erdölerzeugnissen, die in Anhang XIII aufgeführt sind, ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, sowie Anwendung irregulärer und mit hohem Risiko behafteter Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation', 'EU'),
('NS Lotus', 9339337, 'Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe c: so betrieben werden, dass sie zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beitragen oder diese unterstützen.', 'EU'),
('NS Spirit', 9318553, 'Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe c: so betrieben werden, dass sie zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beitragen oder diese unterstützen.', 'EU'),
('NS Stream', 9318541, 'Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe c: so betrieben werden, dass sie zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beitragen oder diese unterstützen.', 'EU'),
('SCF Amur', 9333436, 'Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe c: so betrieben werden, dass sie zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beitragen oder diese unterstützen oder von Russland strukturell für Zwecke des Energietransports genutzt werden, was dem Ziel der Verringerung der russischen Einnahmen in diesem Sektor zuwiderläuft', 'EU'),
('Lady R', 9161003, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe a: im Verteidigungs- und Sicherheitssektor verwendete Güter und Technologien von oder nach Russland, zur Verwendung in Russland oder für die Kriegsführung Russlands in der Ukraine befördern', 'EU'),
('Maia-1', 9358010, 'Crude Oil Tanker', 'Russia', 'Artikel 4x, Absatz 2 Buchstabe a: im Verteidigungs- und Sicherheitssektor verwendete Güter und Technologien von oder nach Russland, zur Verwendung in Russland oder für die Kriegsführung Russlands in der Ukraine befördern', 'EU'),
('Audax', 9763837, 'Crude Oil Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe c: Betrieb derart, dass zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beigetragen wird oder diese unterstützt werden', 'EU'),
('Pugnax', 9763849, 'Crude Oil Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe c: Betrieb derart, dass zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beigetragen wird oder diese unterstützt werden', 'EU'),
('Hunter Star', 9830769, 'Crude Oil Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe c: Betrieb derart, dass zu Maßnahmen oder Strategien zur Ausbeutung, zur Entwicklung oder zum Ausbau des Energiesektors in Russland, einschließlich der Energieinfrastruktur, beigetragen wird oder diese unterstützt', 'EU'),
('Hebe', 9259185, 'Crude Oil Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Enisey', 9079169, 'Bulk Carrier', 'Russia', 'Artikel 4x Absatz 2 Buchstabe d Betrieb derart, dass zu Maßnahmen oder Strategien beigetragen wird oder diese unterstützt werden, mit denen die wirtschaftliche Lebensfähigkeit oder die Ernährungssicherheit der Ukraine (etwa durch Beförderung gestohlenen ukrainischen Getreides) oder die Erhaltung des kulturellen Erbes der Ukraine (etwa durch Beförderung gestohlener ukrainischer Kulturgüter) untergraben oder bedroht werden', 'EU'),
('Vela Rain', 9331141, 'Crude Oil Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Ocean AMZ', 9394935, 'Crude Oil Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Galian 2', 9331153, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Robon', 9144782, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Beks Aqua', 9277735, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Kemerovo', 9312884, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe b: Beförderung von in Anhang XIII aufgeführten Rohöl oder Erdölerzeugnissen, die ihren Ursprung in Russland haben oder aus Russland ausgeführt werden, während irreguläre und mit hohem Risiko behaftete Transportpraktiken gemäß der Entschließung A.1192(33) der Generalversammlung der Internationalen Seeschifffahrtsorganisation betrieben werden.', 'EU'),
('Krymsk', 9270529, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe g: Schiff, das Eigentum der in Anhang I der Verordnung (EU) Nr. 269/2014 aufgeführten natürlichen oder juristischen Personen, Organisationen oder Einrichtungen ist, von diesen gechartert oder betrieben wird oder anderweitig unter dem Namen oder im Namen, in Verbindung mit diesen oder zugunsten dieser Personen verwendet wird', 'EU'),
('Krasnoyarsk', 9312896, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe g: Schiff, das Eigentum der in Anhang I der Verordnung (EU) Nr. 269/2014 aufgeführten natürlichen oder juristischen Personen, Organisationen oder Einrichtungen ist, von diesen gechartert oder betrieben wird oder anderweitig unter dem Namen oder im Namen, in Verbindung mit diesen oder zugunsten dieser Personen verwendet wird', 'EU'),
('Kaliningrad', 9341067, 'Tanker', 'Russia', 'Artikel 4x Absatz 2 Buchstabe g: Schiff, das Eigentum der in Anhang I der Verordnung (EU) Nr. 269/2014 aufgeführten natürlichen oder juristischen Personen, Organisationen oder Einrichtungen ist, von diesen gechartert oder betrieben wird oder anderweitig unter dem Namen oder im Namen, in Verbindung mit diesen oder zugunsten dieser Personen verwendet wird', 'EU');

CREATE TABLE baltic_sea_harbours (
    name STRING,
    country STRING,
    latitude DOUBLE,
    longitude DOUBLE
);


INSERT INTO baltic_sea_harbours VALUES
('Stockholm', 'Sweden', 59.3293, 18.0686),
('Helsinki', 'Finland', 60.1695, 24.9354),
('Tallinn', 'Estonia', 59.4370, 24.7536),
('Riga', 'Latvia', 56.9496, 24.1052),
('Gdynia', 'Poland', 54.5189, 18.5305),
('Klaipėda', 'Lithuania', 55.7033, 21.1443),
('Turku', 'Finland', 60.4518, 22.2666),
('Mariehamn', 'Finland', 60.0973, 19.9348),
('Liepāja', 'Latvia', 56.5110, 21.0136),
('Ventspils', 'Latvia', 57.3890, 21.5610),
('Kaliningrad', 'Russia', 54.7104, 20.4522),
('Świnoujście', 'Poland', 53.9106, 14.2478),
('Rostock', 'Germany', 54.0887, 12.1405),
('Travemünde', 'Germany', 53.9624, 10.8672),
('St. Petersburg', 'Russia', 59.9343, 30.3351),
('Karlskrona', 'Sweden', 56.1612, 15.5869),
('Kiel', 'Germany', 54.3233, 10.1228),
('Wismar', 'Germany', 53.8934, 11.4536),
('Stralsund', 'Germany', 54.3091, 13.0810),
('Sassnitz', 'Germany', 54.5183, 13.6414),
('Greifswald', 'Germany', 54.0934, 13.3781),
('Nynäshamn', 'Sweden', 58.9036, 17.9470),
('Ustka', 'Poland', 54.5801, 16.8596),
('Pori', 'Finland', 61.4847, 21.7976),
('Kemi', 'Finland', 65.7369, 24.5636),
('Gdańsk', 'Poland', 54.3520, 18.6466),
('Paldiski', 'Estonia', 59.3567, 24.0539),
('Rønne', 'Denmark', 55.1037, 14.7065),
('Visby', 'Sweden', 57.6409, 18.2960),
('Bolderāja', 'Latvia', 56.9950, 24.0500),
('Primorsk', 'Russia', 60.3565, 28.6094);

