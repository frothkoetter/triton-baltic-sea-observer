# Download today’s S3 artifacts (AWS CLI)

Script: [`download-today-s3-artifacts.sh`](./download-today-s3-artifacts.sh)

Downloads objects whose **S3 `LastModified`** falls on the chosen calendar day in **`ARTIFACT_TZ`** (default `Europe/Berlin`):

| Category | Env prefix | Extensions |
|----------|------------|------------|
| SITREP | `SITREP_S3_PREFIX` or `LAGEBILD_S3_PREFIX` | `.pdf` (+ `index.json` when present) |
| LiDAR | `S3_LIDAR_ALLOWED_PREFIX` | `.las`, `.laz` |
| DAS | `S3_DAS_ALLOWED_PREFIX` | `.h5`, `.hdf5`, `.hdf`, `.wav` |

## Prerequisites

- AWS CLI v2 (`aws`)
- CDP workload credentials: `S3_WORKLOAD_USER` / `S3_WORKLOAD_PASSWORD` (or `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`)
- `S3_ENDPOINT_URL` for Ozone / CDP object store (same as **triton-ui** Application)

## Example (CML project session)

```bash
cd /home/cdsw/triton-baltic-sea-observer   # or your clone path

export S3_WORKLOAD_USER='...'
export S3_WORKLOAD_PASSWORD='...'
export S3_ENDPOINT_URL='https://...'     # from CDP object store docs

export SITREP_S3_PREFIX='s3a://goes-se-sandbox/data/user/you/reports/'
export S3_LIDAR_ALLOWED_PREFIX='s3a://goes-se-sandbox/data/geospatial/transformer/'
export S3_DAS_ALLOWED_PREFIX='s3a://goes-se-sandbox/data/acoustic/'

chmod +x scripts/download-today-s3-artifacts.sh
./scripts/download-today-s3-artifacts.sh
```

Or load project env:

```bash
./scripts/download-today-s3-artifacts.sh \
  --env-file scripts/cml/triton-project.env.local
```

Output default: `triton-s3-artifacts-YYYYMMDD/` plus **`triton-s3-artifacts-YYYYMMDD.tar.gz`** in the repo root (override with `--out`).

## Options

```bash
./scripts/download-today-s3-artifacts.sh --date 2026-11-24 --dry-run
./scripts/download-today-s3-artifacts.sh --out /tmp/heute --no-archive
```

## Notes

- Large LAS/HDF5 files can take time and disk; use `--dry-run` first.
- URIs may use `s3://` or `s3a://` (NiFi / Iceberg style).
- If a prefix is unset, that category is skipped (not an error).