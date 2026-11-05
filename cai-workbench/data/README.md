# Local data for Cloudera AI Workbench projects

Place project-specific datasets here so CML sessions and jobs can use **paths inside the synced git tree** (instead of only S3 or `/tmp`).

| Directory | Used by | Environment / CLI |
|-----------|---------|-------------------|
| `lidar/` | `lidar-vessel-classifier` | `LIDAR_LOCAL_DIR=$PWD/data/lidar` or `fit.py --local-dir data/lidar` |
| `ais/` | `ais-outlier-detector` | `run_local.py < data/ais/*.ndjson` or NiFi batch replay |
| `lagebild/` | `lagebild.py` / notebook | Optional CSV/Parquet exports from `defense.lagebild` for offline prompts |

## LiDAR

- Per-vessel `.las` files named `sample_<vessel_type>.las` match `src/s3_las_loader.label_from_key`.
- Large fleet dumps (`baltic_fleet*.las`, `all_vessel_types_enu.las`) stay under `nifi-processors/dev/` or object store; copy only what you need into `data/lidar/`.

## AIS

- One JSON object per line (NDJSON), same fields as NiFi `ShipSimulationRoutesGemini` / `AisOutlierCmlInvoke`.

## Git

Files under `data/` are **meant to be committed** when they are small enough for the repo (samples, fixtures). Generated models stay gitignored (`model.pkl`, `artifacts/model.joblib` in each CML subproject).
