# LiDAR vessel classifier (Cloudera AI Workbench)

Train a scikit-learn model on `.las` point clouds in S3 and deploy it as a **CML Model Deployment** for vessel-type classification (codes 1–9, same as `LidarSimulator`).

## Layout

| Path | Purpose |
|------|---------|
| `train.py` | Load LAS from S3, extract features, train RandomForest, write `artifacts/` |
| `model-deployment/` | CML deployment bundle (`predict.py`, `model.joblib`, `cdsw-build.sh`) |
| `deploy_cml_model.py` | Register model + build + deployment via `cmlapi` |
| `src/features.py` | Point-cloud shape / intensity features |

## 1. CML project setup

1. Create a **Cloudera AI** project (or Workbench) and sync this directory (`cai-workbench/lidar-vessel-classifier/`).
2. Ensure the project can read your object store (instance profile, HDFS/S3 credentials, or environment keys used by `boto3`).
3. The first project build runs `cdsw-build.sh` → installs `requirements.txt`.

**Python runtime (not notebook):** see [RUNTIME.md](RUNTIME.md) — run `python3 train.py` in the **terminal** or as a **Job**; do not paste `train.py` into the interactive Python pane.

## 2. Training data (S3)

Default prefix (override with `LIDAR_S3_PREFIX`):

```text
s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/
```

Each `.las` file should carry **LAS classification** 1–9 (NiFi simulator) or a name like `sample_corvette.las`.

**Run as a Python script** (CML session terminal or **Job**):

```bash
cd /home/cdsw/lidar-vessel-classifier   # project files directory in Workbench
python3 train.py --s3-prefix "s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/"
```

Or use the job wrapper:

```bash
./run_train.sh
```

**CML Job:** Type = **Job**, Script = `run_train.sh` or `python3 train.py` with arguments, Engine = Python 3.

```bash
python3 train.py --help
python3 train.py --max-files 500
python3 train.py --local-dir /path/to/samples   # dev only
```

**Recommended:** use a Job or `python3 train.py` from the shell (not notebook cells).

If you must use a **notebook**, `setup_project` is not on `PYTHONPATH` until you bootstrap.

**First cell** (set your real path — often under `/home/cdsw/...`):

```python
import os
os.environ["LIDAR_PROJECT_ROOT"] = "/home/cdsw/lidar-vessel-classifier"
# whole repo clone: "/home/cdsw/triton-baltic-sea-observer/cai-workbench/lidar-vessel-classifier"

%run /home/cdsw/lidar-vessel-classifier/notebook_bootstrap.py
```

Then:

```python
from features import extract_features_from_las_bytes, ground_truth_label_from_las_bytes
```

Or without `%run`, one cell only:

```python
import os, sys
from pathlib import Path
ROOT = Path("/home/cdsw/lidar-vessel-classifier").resolve()
sys.path[:0] = [str(ROOT), str(ROOT / "src")]
from features import extract_features_from_las_bytes
```

Training: `%run train.py` after bootstrap, or use a **Job** with `python3 train.py`.

Optional env vars: `LIDAR_S3_PREFIX`, `LIDAR_LOCAL_DIR`, `LIDAR_MAX_FILES`.

Outputs:

- `artifacts/model.joblib`
- `artifacts/training_rows.csv`
- `model-deployment/` (copy for serving)

## 3. Model deployment (CML)

After a successful `train.py`, from a **project session** with API access:

```bash
export CML_MODEL_NAME="lidar-vessel-classifier"
export CML_DEPLOYMENT_NAME="lidar-vessel-classifier-deployment"
python deploy_cml_model.py
```

`CDSW_PROJECT_ID` is set automatically in CML. The script:

1. Creates or reuses a model registry entry  
2. Starts a **model build** from `model-deployment/`  
3. Waits for build success (`CML_WAIT_FOR_BUILD=true`)  
4. Creates a deployment (CPU/memory/replicas via env vars)

### Invoke the endpoint

Body (JSON):

```json
{
  "s3_uri": "s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/<uuid-or-file>.las"
}
```

Or inline file:

```json
{
  "las_bytes_base64": "<base64-encoded .las>"
}
```

Response:

```json
{
  "vessel_type": "corvette",
  "confidence": 0.91,
  "probabilities": { "corvette": 0.91, "frigate": 0.04, ... }
}
```

Use the deployment URL from the CML UI or `deploy_cml_model.py` output, with your CML API token.

## 4. Manual UI alternative

If `cmlapi` build fields differ on your CML version:

1. **Model** → **New Model** → name `lidar-vessel-classifier`  
2. **Build** → source directory `model-deployment`, kernel Python 3  
3. **Deploy** → select successful build → set replicas / auth  

Entry function: `predict` in `model-deployment/predict.py`.

## Classes

| Code | Type |
|------|------|
| 1 | oil_tanker |
| 2 | gas_tanker |
| 3 | container_ship |
| 4 | ferry |
| 5 | sailing |
| 6 | corvette |
| 7 | frigate |
| 8 | submarine |
| 9 | landing_ship |

## Notes

- Features are **file-level** (whole scene); multi-vessel LAS files use the **majority** classification code as label.
- For production accuracy, train on many simulator runs per class in S3, not only nine sample files.
- Deployment needs S3 read access if you pass `s3_uri` at inference time.
