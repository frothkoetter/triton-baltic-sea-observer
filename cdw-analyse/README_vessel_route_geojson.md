# Vessel AIS routes as GeoJSON (Trino)

Build **GeoJSON `Feature`** objects per **MMSI** from `ais_events_ice`: a **Route** with start/end timestamp, start/end port (nearest `baltic_sea_harbours` or AIS `Destination`), and a **LineString** of AIS positions (sampled for size).

## Objects

| Artifact | Description |
|----------|-------------|
| [`vessel_route_geojson.sql`](vessel_route_geojson.sql) | `CREATE VIEW vessel_ais_route_geojson` |
| [`run_vessel_route_geojson.sh`](run_vessel_route_geojson.sh) | Deploy view + print one sample route |

## GeoJSON shape

```json
{
  "type": "Feature",
  "properties": {
    "type": "Route",
    "mmsi": 311040004,
    "start_timestamp": "2026-11-16 10:51:14.317 UTC",
    "end_timestamp": "2026-11-16 16:24:48.972 UTC",
    "start_port": "Warnemünde",
    "end_port": "Nynäshamn",
    "point_count": 104
  },
  "geometry": {
    "type": "LineString",
    "coordinates": [[lon, lat], ...]
  }
}
```

Ports: closest row in `baltic_sea_harbours` to the **first** / **last** AIS point in the 24h window (includes North Sea hubs such as **Emden**).

## Run (CDW)

```bash
cd cdw-analyse
source .env.trino.local   # or set -a && source ...
./run_vessel_route_geojson.sh 311040004
```

## Queries

```sql
USE iceberg.defense;

-- One vessel
SELECT route_geojson
FROM vessel_ais_route_geojson
WHERE mmsi = 311040004;

-- FeatureCollection for a map (limit routes)
SELECT format(
  '{"type":"FeatureCollection","features":[%s]}',
  array_join(array_agg(route_geojson), ',')
)
FROM (
  SELECT route_geojson
  FROM vessel_ais_route_geojson
  ORDER BY mmsi
  LIMIT 50
);
```

## Live system check (Nov 2026)

On `cdw-triton` with ~328k `ais_events_ice` rows, a single NSB vessel (`311040004`) produced a valid Route Feature (~104 coordinates, Warnemünde → Nynäshamn over ~5.5h).

## Tuning

- Window: `interval '24' hour` in the view definition.
- Simplification: `n / 120` in `sampled` (≈120 points max per track).
- Change the view and re-run `run_vessel_route_geojson.sh`.

## Note

Trino on this cluster does not expose `ST_AsGeoJSON`; coordinates are assembled as RFC 7946 JSON manually.