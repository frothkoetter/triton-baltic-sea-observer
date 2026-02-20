CREATE TABLE `ssb`.`ssb_default`.`ais_ports_monitoring_ice` (
 `window_start` TIMESTAMP(3) NOT NULL,
 `window_end` TIMESTAMP(3),
 `MMSI` BIGINT NOT NULL,
 `Latitude` DOUBLE,
 `Longitude` DOUBLE,
 `dist_to_port_km` DOUBLE,
 `port_name` VARCHAR(2147483647),
 `event_date` DATE NOT NULL,
 CONSTRAINT `PK_event_date_window_start_MMSI` PRIMARY KEY (`event_date`, `window_start`, `MMSI`) NOT ENFORCED
) PARTITIONED BY (`event_date`)
WITH (
 'engine.hive.enabled' = 'true',
 'hive-conf-dir' = '/etc/hive/conf',
 'connector' = 'iceberg',
 'catalog-database' = 'defense',
 'write.upsert.enabled' = 'true',
 'catalog-table' = 'ais_ports_monitoring_ice',
 'format' = 'parquet',
 'catalog-type' = 'hive',
 'catalog-name' = 'Hive'
)



INSERT INTO `ssb`.`ssb_default`.`ais_ports_monitoring_ice`
SELECT 
    TUMBLE_START(ais.eventTimestamp, INTERVAL '5' MINUTE) AS window_start,
    TUMBLE_END(ais.eventTimestamp, INTERVAL '5' MINUTE) AS window_end,
    ais.MMSI,
    ais.Latitude,
    ais.Longitude,
    -- Spatial Distance Calculation to joined port
    ORG_APACHE_SEDONA_FLINK_EXPRESSIONS_FUNCTIONS$ST_DISTANCESPHERE(
        ORG_APACHE_SEDONA_FLINK_EXPRESSIONS_CONSTRUCTORS$ST_POINT(ais.Longitude, ais.Latitude), 
        ORG_APACHE_SEDONA_FLINK_EXPRESSIONS_CONSTRUCTORS$ST_POINT(ports.longitude, ports.latitude)
    ) / 1000 AS dist_to_port_km,
    ports.name AS port_name,
    CAST(TUMBLE_START(ais.eventTimestamp, INTERVAL '5' MINUTE) AS DATE) AS event_date
FROM `ssb`.`ssb_default`.`ais_events_record` AS ais
CROSS JOIN `ssb`.`ssb_default`.`ports` AS ports
WHERE 
    -- Filter: Only records where ship is within 50km of the port
    ORG_APACHE_SEDONA_FLINK_EXPRESSIONS_FUNCTIONS$ST_DISTANCESPHERE(
        ORG_APACHE_SEDONA_FLINK_EXPRESSIONS_CONSTRUCTORS$ST_POINT(ais.Longitude, ais.Latitude), 
        ORG_APACHE_SEDONA_FLINK_EXPRESSIONS_CONSTRUCTORS$ST_POINT(ports.longitude, ports.latitude)
    ) <= 5000
GROUP BY 
    TUMBLE(ais.eventTimestamp, INTERVAL '5' MINUTE), 
    ais.MMSI, 
    ais.Latitude, 
    ais.Longitude, 
    ports.name, 
    ports.latitude, 
    ports.longitude;

