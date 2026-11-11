# Local data for Cloudera AI Workbench projects

Place project-specific datasets here so CML sessions and jobs can use **paths inside the synced git tree** (instead of only S3 or `/tmp`).

| Directory | Used by | Environment / CLI |
|-----------|---------|-------------------|
| *(LiDAR samples live under `lidar-vessel-classifier/data/lidar/`)* | `lidar-vessel-classifier` | `LIDAR_LOCAL_DIR=$PWD/data/lidar` inside that CML project |
| `ais/` | `ais-outlier-detector` | `run_local.py < data/ais/*.ndjson` or NiFi batch replay |
| `lagebild/` | `situation-awareness/` | `sample_lagebild.csv` for offline notebook/job; optional Parquet exports from `defense.lagebild` |

## LiDAR

- Sample `.las` files are committed under **`lidar-vessel-classifier/data/lidar/`** (includes `sample_fishing_boat.las`, class 11).
- Large fleet dumps stay under `nifi-processors/dev/` or object store.

## AIS

- One JSON object per line (NDJSON), same fields as NiFi `ShipSimulationRoutesGemini`.

## Git

Files under `data/` are **meant to be committed** when they are small enough for the repo (samples, fixtures). Generated models stay gitignored (`model.pkl`, `artifacts/model.joblib` in each CML subproject).
