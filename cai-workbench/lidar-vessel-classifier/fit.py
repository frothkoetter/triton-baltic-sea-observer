# CML Experiment entry — trains, logs MLflow metrics/artifacts, writes model.pkl for predict.py.
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
from src.mlflow_tracking import log_training_run, mlflow_enabled

try:
    import cml.metrics_v1 as metrics
except ImportError:
    metrics = None


def main() -> None:
    argv = [a for a in sys.argv[1:] if "kernel-" not in a or not a.endswith(".json")]
    train.main(argv if argv else None)
    meta_path = Path("artifacts/training_meta.json")
    meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.is_file() else {}

    if metrics and meta:
        metrics.track_metric("n_samples", float(meta.get("n_samples", 0)))
        metrics.track_metric("n_labels", float(len(meta.get("labels", []))))
        for key in ("accuracy", "macro_f1"):
            if key in meta.get("metrics", {}):
                metrics.track_metric(key, float(meta["metrics"][key]))

    if mlflow_enabled():
        import joblib

        pipeline = joblib.load(Path("artifacts/model.joblib"))
        m = meta.get("metrics", {})
        rf = meta.get("hyperparameters", {})
        run_info = log_training_run(
            pipeline,
            params={
                "classifier": "RandomForestClassifier",
                "eval_method": meta.get("eval_method", "unknown"),
                "cv_n_splits": meta.get("cv_n_splits", 0),
                **{f"rf_{k}": v for k, v in rf.items()},
            },
            metrics={k: float(v) for k, v in m.items()},
            labels=meta.get("labels", []),
            artifact_dir=Path("artifacts"),
            run_name=os.environ.get("MLFLOW_RUN_NAME"),
            tags={
                "source": "fit.py",
                "project": "lidar-vessel-classifier",
            },
        )
        if run_info:
            print(f"MLflow run id: {run_info}")
    elif not os.environ.get("LIDAR_MLFLOW_DISABLED"):
        print("MLflow not installed; skip experiment logging (pip install mlflow or use CML session).")


if __name__ == "__main__":
    main()
