# LiDAR vessel classifier (Cloudera AI)

Train and deploy a vessel-type classifier on LiDAR `.las` data (Cloudera template: `fit.py` + `predict.py`).

| File | Role |
|------|------|
| `cdsw-build.sh` | Model/experiment build — `pip install -r requirements.txt` |
| `fit.py` | Experiment — train, write `model.pkl` |
| `train.py` | Same training with CLI flags |
| `predict.py` | Model deployment — function `predict` (`s3_uri` or `las_bytes_base64`) |
| `classify.py` | Model deployment — function `classify` (`s3_uri` only) |
| `predict_with_metrics.py` | Model with `wrap_prediction` metrics (for `use_model_metrics.py`) |
| `use_model_metrics.py` | Demo: call model, read/track delayed and aggregate metrics |
| `entry.py` | CDSW application static file server (`CDSW_APP_PORT`) |
| `lineage.yaml` | Optional model lineage metadata |
| `model.pkl` | Fitted classifier (from `fit.py` / `train.py`) |
| `src/` | `features.py`, `labels.py`, `s3_client.py`, `s3_las_loader.py` |

S3 uses the **S3 Object Store** data connection (`LIDAR_S3_CONNECTION_NAME`, default `S3 Object Store`).

Default data prefix: `s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/`

## Train

Run experiment **`fit.py`**, or:

```bash
cd "${CDSW_WORKING_DIRECTORY}"
python3 fit.py
```

Env: `LIDAR_S3_PREFIX`, `LIDAR_LOCAL_DIR`, `LIDAR_MAX_FILES`.

## Deploy

**Option A — `predict.py`** (recommended): entry function **`predict`**.

```json
{"s3_uri": "s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/<key>"}
```

or `{"las_bytes_base64": "..."}`.

**Option B — `classify.py`**: entry function **`classify`**, input `{"s3_uri": "s3://..."}` only.

Run **`cdsw-build.sh`** (or install `requirements.txt`) before deploying so `laspy`, `boto3`, and `scikit-learn` are available.

## Metrics

Deploy **`predict_with_metrics.py`** with entry function **`predict`**, then run **`use_model_metrics.py`** in a session (`dev = True` for local calls).
