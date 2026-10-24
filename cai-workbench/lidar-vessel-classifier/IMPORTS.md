# Imports in a Cloudera AI Python session

Set project root first (folder containing `train.py`):

```python
import os, sys
sys.path.insert(0, os.environ["CDSW_WORKING_DIRECTORY"])
```

### `from features import ...`

```python
from features import extract_features_from_las_bytes, ground_truth_label_from_las_bytes
```

### `from src.features import ...`

```python
from src.features import extract_features_from_las_bytes, ground_truth_label_from_las_bytes
```

### Project path without `__file__`

```python
from paths import ROOT
_SCRIPT_DIR = ROOT
```

### Training

```python
import train
train.main()
```

Or use **Terminal**: `python3 train.py`
