# Sample LAS for local training

Per-vessel files: `sample_<vessel_type>.las` (parsed by `src/s3_las_loader.label_from_key`).

| File | LAS hull class |
|------|----------------|
| `sample_fishing_boat.las` | 11 |
| Other `sample_*.las` | 1–9 (see `ontology/enums/lidar_classification_codes.yaml`) |

Train from the project root:

```bash
export LIDAR_LOCAL_DIR="${CDSW_WORKING_DIRECTORY}/data/lidar"
python3 fit.py
```

Regenerate fishing sample (repo root, needs laspy):

```bash
PYTHONPATH=nifi-processors/upload python3 -c "
from pathlib import Path
from LidarSimulator import build_single_vessel_points, write_baltic_lidar_enu
p = Path('cai-workbench/lidar-vessel-classifier/data/lidar/sample_fishing_boat.las')
pts, cls, *rest = build_single_vessel_points(seed=42, target_vessel_type='fishing_boat', unclassified_fraction=0.0)
write_baltic_lidar_enu(str(p), points=pts, classification=cls)
"
```
