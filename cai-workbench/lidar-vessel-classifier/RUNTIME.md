# Cloudera AI — Python runtime

## `Cell In[n]` is not a separate “notebook product”

In Cloudera AI Workbench / ML, the **Python** session is still **IPython**. The prompt `Cell In[20]` is normal there. It is **not** the same as running `python3 train.py` in a **Terminal** tab.

| You see | What to do |
|---------|------------|
| `Cell In[n]` | `import train; train.main()` after `sys.path` setup (below), or use **Terminal** |
| `>>>` in Terminal | `python3 train.py ...` |

There is **no `__file__`** when you type or paste one line at a time in a Python session cell. Do not copy `_SCRIPT_DIR = Path(__file__)…` from `train.py`.

## Install dependencies (once)

```bash
python3 -m pip install --user -r requirements-train.txt
```

## Train — Terminal (batch Python)

```bash
cd "${CDSW_WORKING_DIRECTORY}"
python3 train.py --s3-prefix "s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/"
```

## Train — Python session (`Cell In[n]`)

```python
import os, sys
sys.path.insert(0, os.environ["CDSW_WORKING_DIRECTORY"])

import train
train.main()
```

## Project directory in a session (replaces `Path(__file__)`)

```python
import os, sys
sys.path.insert(0, os.environ["CDSW_WORKING_DIRECTORY"])
from paths import ROOT
_SCRIPT_DIR = ROOT
```

## Environment variables

`LIDAR_S3_PREFIX`, `LIDAR_LOCAL_DIR`, `LIDAR_MAX_FILES`, `CDSW_WORKING_DIRECTORY`
