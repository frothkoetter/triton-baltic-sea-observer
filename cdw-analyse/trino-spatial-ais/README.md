# Trino räumliche Analyse (WKT) + AIS + Pipelines

Implementierung des Drafts **„Räumliche Analyse in Trino — OGC WKT“** (Nord Stream / Balticconnector-Szenario: **LINESTRING**-Infrastruktur vs. **AIS-Trajektorien** mit `ST_Intersects`).

- **Volltext:** [`docs/implementation-draft-de.md`](docs/implementation-draft-de.md) (aus deinem Download)
- **Kurz:** [`IMPLEMENTATION_DRAFT.md`](IMPLEMENTATION_DRAFT.md)
- **Google Doc:** [Link](https://docs.google.com/document/d/1kvir7e4BCon-wKynGXWsOyF5xWviPLi7HY3WngzVlbA/edit?usp=sharing)

Schema: **`iceberg.defense`** (Triton). Koordinaten immer **`longitude latitude`** (WGS84).

## Projektstruktur

| Pfad | Inhalt |
|------|--------|
| `sql/01–05_*.sql` | AOI, Infrastruktur, **GeoJSON staging**, **segmentierte Pipelines** |
| `sql/queries/05_*.sql` | Mehrfach-Pipeline-Überquerungen (LEAD + WKT) |
| `sql/queries/06–08_*.sql` | Korridor, **segmentiert+BBox**, **AIS-Blackouts** |
| `scripts/rebuild_pipelines.sh` | Nur `04`+`05` neu aufbauen |
| `data/sample_ais_pipeline_track.csv` | MMSI entlang Mock-Pipeline |
| `scripts/sync_emodnet_baltic.py` | EMODnet WFS → **WKT**-Parquet/GeoJSON |
| `scripts/download_emodnet.sh` | curl WFS oder Python-Sync |
| `scripts/apply_spatial_lab.sh` | DDL auf Trino |
| `scripts/e2e_trino_ais.py` | E2E inkl. Pipeline-Queries |

## Schnellstart

```bash
cd cdw-analyse/trino-spatial-ais
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest tests/ -q

# Optional Open Data
pip install -r requirements-sync.txt
./scripts/download_emodnet.sh

# Trino
export TRINO_HOST=... TRINO_USER=... TRINO_PASSWORD=...
./scripts/apply_spatial_lab.sh
python scripts/e2e_trino_ais.py --load-sample --load-pipeline-track --load-blackout-sample
```

## OGC / Trino (Draft)

| Objekt | WKT / Funktion |
|--------|----------------|
| Pipeline | `LINESTRING(lon lat, …)` in `geometrie_wkt` |
| AIS-Punkt | `POINT(lon lat)` / `ST_Point(lon, lat)` |
| Fahrtsegment | `ST_LineFromText('LINESTRING(...)')` aus `LEAD` |
| Schnitt | `ST_Intersects(segment, ST_GeometryFromText(wkt))` |
| Korridor | `ST_Buffer(geom, 0.005)` |
| Geodätisch | `to_spherical_geography(ST_Point(...))` |

Zusätzlich AOI: `observation_areas.polygon` als `POLYGON`, View `area_violation`.

Voll-DDL Triton: [`../create_db_tables_trino.sql`](../create_db_tables_trino.sql)
