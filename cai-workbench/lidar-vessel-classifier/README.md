# LiDAR vessel classifier (Cloudera AI)

Train and deploy a vessel-type classifier on LiDAR `.las` data (Cloudera template: `fit.py` + `predict.py`).

| File | Role |
|------|------|
| `fit.py` | Experiment — train, write `model.pkl` |
| `predict.py` | Model deployment — function `predict` |
| `train.py` | Same training with CLI flags |
| `src/` | Features, S3 loader, labels |

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

Models → **`predict.py`** → entry function **`predict`**.

```json
{"s3_uri": "s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/<key>"}
```

or `{"las_bytes_base64": "..."}`.
