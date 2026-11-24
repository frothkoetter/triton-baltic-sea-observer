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
- **Auth (pick one):**
  - **AWS SSO** (recommended on laptop): profile in `~/.aws/config`, `aws sso login --profile …`
  - **CDP workload keys:** `S3_WORKLOAD_USER` / `S3_WORKLOAD_PASSWORD` or `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
- **`S3_ENDPOINT_URL`** for CDP / Ozone object store (required for private cloud buckets; same idea as **triton-ui**)

### SSO profile (`~/.aws/config` sketch)

```ini
[profile triton-cdp]
sso_start_url = https://your-org.awsapps.com/start
sso_region = eu-central-1
sso_account_id = 123456789012
sso_role_name = YourRole
region = eu-central-1
```

## Bucket layout (`triton-buk-ab3b8c81`)

Under `s3a://triton-buk-ab3b8c81/data/user/frothkoetter/` the three folders map to env vars (and **triton-ui**):

| S3 folder | Download script env | UI env (same prefix) |
|-----------|---------------------|----------------------|
| `reports/` | `SITREP_S3_PREFIX` or `LAGEBILD_S3_PREFIX` | `SITREP_S3_PREFIX`, `LAGEBILD_S3_PREFIX` |
| `lidar/` | `S3_LIDAR_ALLOWED_PREFIX` | `S3_LIDAR_ALLOWED_PREFIX` |
| `das/` | `S3_DAS_ALLOWED_PREFIX` | `S3_DAS_ALLOWED_PREFIX` |

Local output dirs are always `sitrep/`, `lidar/`, `das/` under `--out` (independent of the S3 folder names).

## Example — AWS SSO (`triton-buk`)

```bash
aws sso login --profile triton-cdp

export AWS_DEFAULT_REGION=us-east-1
export S3_ENDPOINT_URL='https://...'   # same gateway as CML "S3 Object Store"

export SITREP_S3_PREFIX='s3a://triton-buk-ab3b8c81/data/user/frothkoetter/reports/'
export S3_LIDAR_ALLOWED_PREFIX='s3a://triton-buk-ab3b8c81/data/user/frothkoetter/lidar/'
export S3_DAS_ALLOWED_PREFIX='s3a://triton-buk-ab3b8c81/data/user/frothkoetter/das/'

./scripts/download-today-s3-artifacts.sh --profile triton-cdp
# or one-shot login + download:
./scripts/download-today-s3-artifacts.sh --profile triton-cdp --sso-login
```

Generic bucket (other paths):

```bash
export SITREP_S3_PREFIX='s3a://your-bucket/data/user/you/reports/'
export S3_LIDAR_ALLOWED_PREFIX='s3a://your-bucket/data/user/you/lidar/'
export S3_DAS_ALLOWED_PREFIX='s3a://your-bucket/data/user/you/das/'
```

## Example — workload keys (CML session)

```bash
export S3_WORKLOAD_USER='...'
export S3_WORKLOAD_PASSWORD='...'
export S3_ENDPOINT_URL='https://...'
# ... prefixes as above
./scripts/download-today-s3-artifacts.sh
```

Or load non-secret prefixes from project env (still use SSO profile or keys for auth):

```bash
./scripts/download-today-s3-artifacts.sh \
  --env-file scripts/cml/triton-project.env.local \
  --profile triton-cdp
```

Output default: `triton-s3-artifacts-YYYYMMDD/` plus **`triton-s3-artifacts-YYYYMMDD.tar.gz`** in the repo root (override with `--out`).

## Options

```bash
./scripts/download-today-s3-artifacts.sh --profile triton-cdp --date 2026-11-24 --dry-run
./scripts/download-today-s3-artifacts.sh --profile triton-cdp --out /tmp/heute --no-archive
```

## Notes

- Large LAS/HDF5 files can take time and disk; use `--dry-run` first.
- URIs may use `s3://` or `s3a://` (NiFi / Iceberg style).
- If a prefix is unset, that category is skipped (not an error).