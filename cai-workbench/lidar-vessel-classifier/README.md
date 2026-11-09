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

**Local git data:** sample `.las` files live in [`../data/lidar/`](../data/lidar/) (sync with the repo). In a CML project rooted at `lidar-vessel-classifier/`:

```bash
export LIDAR_LOCAL_DIR="${CDSW_WORKING_DIRECTORY}/../data/lidar"
python3 fit.py
```


Run experiment **`fit.py`**, or:

```bash
cd "${CDSW_WORKING_DIRECTORY}"
python3 fit.py
```

Env: `LIDAR_S3_PREFIX`, `LIDAR_LOCAL_DIR`, `LIDAR_MAX_FILES`.

## Deploy

Deploy **`classify.py`** with entry function **`classify`**. Request body:

```json
{"s3_uri": "s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/<key>"}
```

Run **`cdsw-build.sh`** (or install `requirements.txt`) before deploying so `laspy`, `boto3`, and `scikit-learn` are available.

## Metrics

Deploy **`predict_with_metrics.py`** with entry function **`predict`**, then run **`use_model_metrics.py`** in a session (`dev = True` for local calls).

## NiFi (InvokeHTTP)

Production flows call the **CML Model Service** with **`accessKey`** on the query string; optional **`Authorization: Bearer <model API key>`** if your gateway requires it (same as local `f.sh`). See **[NIFI_INVOKEHTTP.md](./NIFI_INVOKEHTTP.md)** and **`examples/invoke_model_service.sh`**.

If InvokeHTTP reports **`Connect timed out`**, test curl from the **NiFi host**—laptop success does not imply cluster egress to `modelservice.ml-*.cloudera.site`.

## Model Service: `context deadline exceeded`

This comes from the **CML Model API gateway** (request took longer than the deployment timeout). Training on more LAS files does **not** slow `predict`/`classify` much; the usual causes are:

1. **Large or remote S3 object** — the replica downloads the full `.las` before feature extraction. Use a bucket the **S3 Object Store** connection can read (same account/region as training when possible).
2. **Huge point clouds** — set env on the model deployment: **`LIDAR_INFER_MAX_POINTS`** (default `50000`) to subsample before features.
3. **Gateway timeout too low** — in CML, open the deployed model → **Settings** (or replica resources) and increase **request / inference timeout** (e.g. 120–300 s) while testing; then tune down.
4. **Cold start** — first call after deploy loads `model.pkl`; retry once.

Test Model body (wrapped by the service):

```json
{"s3_uri": "s3://<bucket>/path/to/file.las"}
```

Sanity check from a CML session on the same project:

```python
from classify import classify
classify({"s3_uri": "s3://…"})
```

If that is fast but Model Service fails, raise the gateway timeout; if that is slow, fix S3 URI or reduce file size.
