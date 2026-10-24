# One cell — Cloudera AI Python (`Cell In[n]`)

S3 reads use your workbench connection **S3 Object Store** (`cml.data_v1`), same as:

```python
import cml.data_v1 as cmldata
client = cmldata.get_connection("S3 Object Store").get_base_connection()
```

Run **all** of this in **one** cell (select all → run):

```python
import os, sys

sys.path.insert(0, os.environ["CDSW_WORKING_DIRECTORY"])
os.environ.setdefault("LIDAR_S3_CONNECTION_NAME", "S3 Object Store")

import train
train.main()
```

Do **not** run `from paths import …` or `Path(__file__)` in separate cells before this.

## Terminal (preferred)

```bash
cd "${CDSW_WORKING_DIRECTORY}"
python3 train.py --s3-prefix "s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/"
```
