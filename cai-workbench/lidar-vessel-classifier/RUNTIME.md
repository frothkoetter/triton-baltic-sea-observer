# Cloudera AI — run as a Python script

## Install dependencies (once)

```bash
cd "${CDSW_WORKING_DIRECTORY}"
python3 -m pip install --user -r requirements-train.txt
```

Or trigger a **project rebuild** (`cdsw-build.sh`).

`train.py` can auto-install on first run unless `LIDAR_SKIP_PIP_INSTALL=1`.

## Train

```bash
cd "${CDSW_WORKING_DIRECTORY}"
python3 train.py --s3-prefix "s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/"
```

Or `./run_train.sh`.

## CML Job

| Setting | Value |
|---------|--------|
| Command | `python3 train.py --s3-prefix "s3://..."` |
| Working directory | project root |

## Optional environment variables

- `LIDAR_S3_PREFIX`, `LIDAR_LOCAL_DIR`, `LIDAR_MAX_FILES`
- `PYTHONPATH` — only if importing `src/` modules from another script:

```bash
export PYTHONPATH="${CDSW_WORKING_DIRECTORY}/src:${PYTHONPATH}"
```
