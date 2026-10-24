# Imports in a CML code cell

Add the **project root** (folder that contains `train.py` and `src/`), not only `src/`.

## Option A — `from features import` (recommended)

```python
import os, sys
sys.path.insert(0, os.environ["CDSW_WORKING_DIRECTORY"])

from features import extract_features_from_las_bytes, ground_truth_label_from_las_bytes
```

## Option B — `from src.features import`

Same `sys.path` line (project root), then:

```python
from src.features import extract_features_from_las_bytes, ground_truth_label_from_las_bytes
```

## If you only add `src/` to the path

Use flat imports only (do not mix with Option A):

```python
import sys
sys.path.insert(0, "/home/cdsw/lidar-vessel-classifier/src")
from features import extract_features_from_las_bytes, ground_truth_label_from_las_bytes
```

## Training

Use the terminal: `python3 train.py --s3-prefix "s3://..."`
