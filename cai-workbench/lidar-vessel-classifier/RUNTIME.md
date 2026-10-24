# Cloudera AI — run as a Python script

If the UI shows **`Cell In[59]`**, you are in an **IPython cell**, not `python3 train.py`.
`__file__` does not exist there. Do **not** paste lines from `train.py`.

| Goal | Command |
|------|---------|
| Train | Terminal: `python3 train.py --s3-prefix "s3://..."` |
| Train from a cell | `import subprocess, os; subprocess.run(["python3","train.py"], cwd=os.environ["CDSW_WORKING_DIRECTORY"], check=True)` |
| Import `features` in a cell | `import sys; sys.path.insert(0,".../src"); from features import ...` or `from paths import ROOT, SRC` then add `SRC` to `sys.path` |

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
