# Imports in a CML code cell

Run **once** per session:

```python
import os, sys
sys.path.insert(0, os.environ["CDSW_WORKING_DIRECTORY"])

from src.features import extract_features_from_las_bytes, ground_truth_label_from_las_bytes
```

Or:

```python
from paths import install_import_path
install_import_path()
from src.features import extract_features_from_las_bytes, ground_truth_label_from_las_bytes
```

(`paths` requires project root on `sys.path` first — use the `CDSW_WORKING_DIRECTORY` line above.)

Training should still be: `python3 train.py` in the terminal.
