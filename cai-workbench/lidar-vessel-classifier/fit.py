# CML Experiment entry — trains and writes model.pkl for predict.py.
import os
import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])

_root = os.environ.get("CDSW_WORKING_DIRECTORY") or os.getcwd()
os.chdir(_root)
if _root not in sys.path:
    sys.path.insert(0, _root)

import json
from pathlib import Path

import train

try:
    import cml.metrics_v1 as metrics
except ImportError:
    metrics = None


def main() -> None:
    train.main()
    meta_path = Path("artifacts/training_meta.json")
    if metrics and meta_path.is_file():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        metrics.track_metric("n_samples", float(meta["n_samples"]))
        metrics.track_metric("n_labels", float(len(meta.get("labels", []))))


if __name__ == "__main__":
    main()
