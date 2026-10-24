# Cloudera AI Python session (`Cell In[n]`)

**Do not** copy lines from `train.py` or `deploy_cml_model.py` into separate cells (`from project_root …`, `Path(__file__)`, etc.). That will fail.

## Train (one cell)

```python
import os
exec(open(os.path.join(os.environ["CDSW_WORKING_DIRECTORY"], "train_now.py")).read())
```

## Deploy (one cell)

```python
import os
exec(open(os.path.join(os.environ["CDSW_WORKING_DIRECTORY"], "deploy_now.py")).read())
```

S3 training uses connection **S3 Object Store** (`LIDAR_S3_CONNECTION_NAME`).

## Terminal (best)

```bash
cd "${CDSW_WORKING_DIRECTORY}"
python3 train.py --s3-prefix "s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/"
python3 deploy_cml_model.py
```
