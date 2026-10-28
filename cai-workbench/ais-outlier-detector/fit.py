# CML Experiment — train IsolationForest severity model (model.pkl).
import os
import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])

_root = os.environ.get("CDSW_WORKING_DIRECTORY") or os.getcwd()
os.chdir(_root)
if _root not in sys.path:
    sys.path.insert(0, _root)

from src.train_model import main

try:
    import cml.metrics_v1 as metrics
except ImportError:
    metrics = None

if __name__ == "__main__":
    main()
    if metrics:
        import json
        from pathlib import Path

        meta_path = Path("artifacts/training_meta.json")
        if meta_path.is_file():
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            metrics.track_metric("n_samples", float(meta["n_samples"]))
