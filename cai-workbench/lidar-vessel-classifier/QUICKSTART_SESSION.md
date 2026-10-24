# One cell — Cloudera AI Python (`Cell In[n]`)

Run **all** of this in **one** cell (select all → run):

```python
import os, sys

sys.path.insert(0, os.environ["CDSW_WORKING_DIRECTORY"])

import train
train.main()
```

Do **not** run `from paths import …` or `Path(__file__)` in separate cells before this.

## Terminal (preferred)

```bash
cd "${CDSW_WORKING_DIRECTORY}"
python3 train.py --s3-prefix "s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/"
```
