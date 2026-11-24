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
- **AWS SSO** profile (`aws sso login --profile …`) that can list/read your bucket — **no `S3_ENDPOINT_URL` needed** when the profile already reaches object store
- **Or** CDP workload keys (`S3_WORKLOAD_*` / `AWS_ACCESS_KEY_ID`) — often need **`S3_ENDPOINT_URL`** with keys only (see below)

## Bucket layout (`triton-buk-ab3b8c81`)

Under `s3a://triton-buk-ab3b8c81/data/user/frothkoetter/`:

| S3 folder | Download script env |
|-----------|---------------------|
| `reports/` | `SITREP_S3_PREFIX` or `LAGEBILD_S3_PREFIX` |
| `lidar/` | `S3_LIDAR_ALLOWED_PREFIX` |
| `das/` | `S3_DAS_ALLOWED_PREFIX` |

Local output: `sitrep/`, `lidar/`, `das/` under `--out`.

## Example — SSO only (typical)

```bash
aws sso login --profile YOUR_PROFILE

export SITREP_S3_PREFIX='s3a://triton-buk-ab3b8c81/data/user/frothkoetter/reports/'
export S3_LIDAR_ALLOWED_PREFIX='s3a://triton-buk-ab3b8c81/data/user/frothkoetter/lidar/'
export S3_DAS_ALLOWED_PREFIX='s3a://triton-buk-ab3b8c81/data/user/frothkoetter/das/'

./scripts/download-today-s3-artifacts.sh --profile YOUR_PROFILE --dry-run
./scripts/download-today-s3-artifacts.sh --profile YOUR_PROFILE
# one-shot login + download:
./scripts/download-today-s3-artifacts.sh --profile YOUR_PROFILE --sso-login
```

Set **`region = us-east-1`** (or `export AWS_DEFAULT_REGION=us-east-1`) in your profile if signing errors appear.

## Optional: `S3_ENDPOINT_URL`

Only for setups where **`aws s3` does not reach the bucket without an explicit gateway** — usually **workload access keys**, not SSO.

| Auth | `S3_ENDPOINT_URL` |
|------|-------------------|
| **SSO profile** (your case) | **Skip** — SSO + profile handle access |
| Workload / static keys | Often **set** to the CDP S3 gateway URL (from CML **S3 Object Store** connection or admin) |

If unset, the script uses default AWS CLI routing (plus whatever is in `~/.aws/config` for that profile).

## Example — workload keys

```bash
export S3_WORKLOAD_USER='...'
export S3_WORKLOAD_PASSWORD='...'
export S3_ENDPOINT_URL='https://...'   # only with explicit keys
export SITREP_S3_PREFIX='s3a://triton-buk-ab3b8c81/data/user/frothkoetter/reports/'
# … lidar / das prefixes …
./scripts/download-today-s3-artifacts.sh
```

## Options

```bash
./scripts/download-today-s3-artifacts.sh --profile YOUR_PROFILE --date 2026-11-24 --dry-run
./scripts/download-today-s3-artifacts.sh --profile YOUR_PROFILE --out /tmp/heute --no-archive
```

## Notes

- Large LAS/HDF5 files can take time and disk; use `--dry-run` first.
- URIs may use `s3://` or `s3a://`.
- Unset prefix → that category is skipped (not an error).