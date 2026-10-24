# LiDAR vessel classifier (Cloudera AI)

Classify simulated Baltic vessel types from LiDAR `.las` point clouds (S3 or local). Layout follows the **Cloudera AI Python template** (`fit.py` + `predict.py`).

## Files

| File | Purpose |
|------|---------|
| `fit.py` | **Experiment** — train model, write `model.pkl` + `label_names.json` |
| `predict.py` | **Model deployment** — `@models.cml_model` function `predict` |
| `train.py` | Same training logic as `fit.py` (CLI / advanced options) |
| `config.yml` | Project scripts for CML |
| `cdsw-build.sh` | `pip install -r requirements.txt` on project build |
| `src/` | Features, S3 loader, labels |

S3 reads use the workbench data connection **S3 Object Store** (`cml.data_v1`), same as the connector sample.

Default training prefix:

```text
s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/
```

## Train (Experiment)

1. Open the project in Cloudera AI.
2. **Run experiment** with **`fit.py`** as the input script (or Terminal: `python3 fit.py`).
3. When the run succeeds, **`model.pkl`** and **`label_names.json`** are in the project root (also under `artifacts/`).

Optional env: `LIDAR_S3_PREFIX`, `LIDAR_S3_CONNECTION_NAME`, `LIDAR_LOCAL_DIR`, `LIDAR_MAX_FILES`.

## Deploy (Model)

1. After a successful `fit.py` run, open **Models** → **Deploy Model**.
2. Select **`predict.py`** and set the entry function to **`predict`**.
3. Call the endpoint with JSON, for example:

```json
{"s3_uri": "s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/<object-key>"}
```

or

```json
{"las_bytes_base64": "<base64-encoded LAS file>"}
```

Response includes `vessel_type`, `confidence`, and `probabilities`.

## Terminal (batch)

```bash
cd "${CDSW_WORKING_DIRECTORY}"
python3 fit.py
# or
python3 train.py --s3-prefix "s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/"
```

## Optional: API deploy

`deploy_cml_model.py` registers and deploys via `cmlapi` (automation). The UI path above matches the official template and is usually simpler.

## NiFi / Impala

LAS metadata DDL lives in `cdw-analyse/lidar_simulation_metadata_impala.sql` (repo root).
