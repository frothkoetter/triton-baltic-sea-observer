# Implementation Draft — WKT, Pipelines & AIS (Trino)

**Quelle (vollständig):** [`docs/implementation-draft-de.md`](docs/implementation-draft-de.md)  
**Google Doc:** [Implementation Draft — räumliche Analyse in Trino](https://docs.google.com/document/d/1kvir7e4BCon-wKynGXWsOyF5xWviPLi7HY3WngzVlbA/edit?usp=sharing)

Dieses Repo-Paket setzt den Draft auf **`iceberg.defense`** (Triton) um und ergänzt AOI-Analysen (`observation_areas` / `area_violation`).

---

## Kernidee (aus dem Draft)

| Thema | Umsetzung |
|--------|-----------|
| **OGC WKT** | Pipelines/Kabel als `LINESTRING` / `MULTILINESTRING` in `geometrie_wkt`; AIS als `POINT` bzw. **Trajektorien** aus `LEAD`-Segmenten |
| **Überquerung** | Nicht `ST_Intersects(Punkt, Linie)` allein, sondern **`ST_Intersects(Fahrtsegment, Pipeline)`** |
| **Geodaten** | Reihenfolge immer **`lon lat`** (WGS84 / EPSG:4326) |
| **Korridor** | `ST_Buffer(geom, 0.005)` ≈ 500 m; **SOG &lt; 3 kn** = Langsamfahrt/Ankern |
| **Performance** | Segmentierung + Bounding-Box-Vorfilter (siehe Voll-Draft) |
| **Open Data** | EMODnet/HELCOM → `scripts/sync_emodnet_baltic.py`, `scripts/download_emodnet.sh` |
| **Blackouts** | AIS-Lücken 30–1440 min + Nähe/zu Pipeline (SQL im Voll-Draft) |

---

## Artefakte in diesem Projekt

| Schritt | Datei |
|---------|--------|
| AOI + AIS-Basis | `sql/01_schema_spatial_core.sql` |
| `area_violation` | `sql/02_view_area_violation.sql` |
| Infrastruktur WKT + View `ais_daten` | `sql/03_ostsee_infrastruktur.sql` |
| GeoJSON Staging → `ST_GeomFromGeoJSON` | `sql/04_geojson_staging.sql` |
| Segmentierte Pipelines (Performance) | `sql/05_ostsee_pipelines_segmented.sql` |
| Mehrfach-Überquerungen | `sql/queries/05_pipeline_mehrfach_ueberquerungen.sql` |
| Korridor + Langsamfahrt | `sql/queries/06_pipeline_korridor_langsamfahrt.sql` |
| Korridor (segmentiert + BBox) | `sql/queries/07_korridor_segmented_performance.sql` |
| AIS-Blackouts über Pipelines | `sql/queries/08_ais_blackout_pipelines.sql` |
| AIS-Sample (AOI) | `data/sample_ais_positions.csv` |
| AIS-Sample (Pipeline-Track) | `data/sample_ais_pipeline_track.csv` |
| EMODnet-Sync | `scripts/sync_emodnet_baltic.py` |

---

## Mapping Triton ↔ Draft

| Draft | Triton |
|-------|--------|
| `ais_daten` | View auf `ais_events_ice` (`sql/03_…`) |
| `ostsee_infrastruktur` | Tabelle `defense.ostsee_infrastruktur` + Mock `NS1_MOCK` |
| `iceberg.geodaten.*` | Optional: Parquet unter `data/emodnet/` → externes Hive/Iceberg (Draft §3) |

---

## Deploy & Test

```bash
./scripts/apply_spatial_lab.sh          # 01–03 + area_violation
python scripts/e2e_trino_ais.py --load-sample --load-pipeline-track --load-blackout-sample
./scripts/rebuild_pipelines.sh
./scripts/download_emodnet.sh           # Open Data → data/emodnet/
pytest tests/ -q
```

S3/TEXTFILE-Rohimport und Parquet-Bindung: siehe [`docs/implementation-draft-de.md`](docs/implementation-draft-de.md) + `scripts/sync_emodnet_baltic.py`.
