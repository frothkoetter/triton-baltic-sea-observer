# Vessel AIS routes (Trino)

Per-**MMSI** routes from `ais_events_ice` in view `vessel_ais_route_geojson`: route metadata as columns, **LineString** geometry in `geo_json`.

## Objects

| Artifact | Description |
|----------|-------------|
| [`vessel_route_geojson.sql`](vessel_route_geojson.sql) | `CREATE VIEW vessel_ais_route_geojson` |
| [`run_vessel_route_geojson.sh`](run_vessel_route_geojson.sh) | Deploy view + print one sample route |

## View columns

| Column | Description |
|--------|-------------|
| `route_type` | Always `Route` |
| `mmsi` | Vessel MMSI |
| `start_timestamp` / `end_timestamp` | First / last AIS sample in the 24h window |
| `start_port` / `end_port` | Nearest `baltic_sea_harbours` (or AIS destination fallback) |
| `point_count` | Number of vertices in the sampled track |
| `journey_duration` | `end_timestamp - start_timestamp` (interval) |
| `journey_duration_seconds` | Same span in whole seconds |
| `geo_json` | `{"type":"LineString","coordinates":[[lon,lat],...]}` |

Ports use closest row in `baltic_sea_harbours` to the **first** / **last** AIS point (includes North Sea hubs such as **Emden**).

## Run (CDW)

```bash
cd cdw-analyse
source .env.trino.local   # or set -a && source ...
./run_vessel_route_geojson.sh 311040004
```

## Queries

```sql
USE iceberg.defense;

SELECT route_type, mmsi, start_port, end_port, journey_duration, point_count, geo_json
FROM vessel_ais_route_geojson
WHERE mmsi = 311040004;

-- Optional full GeoJSON Feature for a map
SELECT format(
  '{"type":"Feature","properties":{"type":"%s","mmsi":%s,"start_timestamp":"%s","end_timestamp":"%s","start_port":"%s","end_port":"%s","point_count":%s,"journey_duration_seconds":%s},"geometry":%s}',
  route_type,
  cast(mmsi AS varchar),
  cast(start_timestamp AS varchar),
  cast(end_timestamp AS varchar),
  replace(start_port, '"', '\"'),
  replace(end_port, '"', '\"'),
  cast(point_count AS varchar),
  cast(journey_duration_seconds AS varchar),
  geo_json
) AS route_feature
FROM vessel_ais_route_geojson
WHERE mmsi = 311040004;
```

## Tuning

- Window: `interval '24' hour` in the view definition.
- Simplification: `n / 120` in `sampled` (≈120 points max per track).
- Re-apply: `./run_vessel_route_geojson.sh`.

## Note

Trino on this cluster does not expose `ST_AsGeoJSON`; `geo_json` is built with `format` / `transform`.