"""MLflow experiment helpers for Cloudera AI (CDSW / CML)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

DEFAULT_EXPERIMENT = "lidar-vessel-classifier"


def mlflow_enabled() -> bool:
    if os.environ.get("LIDAR_MLFLOW_DISABLED", "").lower() in ("1", "true", "yes"):
        return False
    try:
        import mlflow  # noqa: F401

        return True
    except ImportError:
        return False


def configure_mlflow(experiment_name: str | None = None) -> str:
    import mlflow

    uri = os.environ.get("MLFLOW_TRACKING_URI")
    if uri:
        mlflow.set_tracking_uri(uri)
    name = experiment_name or os.environ.get("MLFLOW_EXPERIMENT_NAME", DEFAULT_EXPERIMENT)
    mlflow.set_experiment(name)
    return name


def log_training_run(
    pipeline,
    *,
    params: dict[str, Any],
    metrics: dict[str, float],
    labels: list[str],
    artifact_dir: Path,
    run_name: str | None = None,
    tags: dict[str, str] | None = None,
) -> str | None:
    """Log parameters, metrics, artifacts, and sklearn model to the active MLflow experiment."""
    import mlflow
    from mlflow.models import infer_signature

    import pandas as pd

    from src.labels import FEATURE_NAMES

    configure_mlflow()
    active = mlflow.start_run(run_name=run_name)
    try:
        if tags:
            mlflow.set_tags(tags)
        mlflow.log_params({k: str(v) for k, v in params.items()})
        for key, value in metrics.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                mlflow.log_metric(key, float(value))

        for rel in ("confusion_matrix.json", "classification_report.json", "training_meta.json"):
            path = artifact_dir / rel
            if path.is_file():
                mlflow.log_artifact(str(path))

        rows_path = artifact_dir / "training_rows.csv"
        signature = None
        if rows_path.is_file():
            df = pd.read_csv(rows_path)
            if len(df) > 0:
                sample = df[FEATURE_NAMES].head(min(5, len(df)))
                signature = infer_signature(sample, pipeline.predict(sample))

        registered = os.environ.get("MLFLOW_REGISTERED_MODEL_NAME") or None
        mlflow.sklearn.log_model(
            pipeline,
            artifact_path="model",
            registered_model_name=registered,
            signature=signature,
            metadata={"labels": json.dumps(labels)},
        )
        if rows_path.is_file():
            mlflow.log_artifact(str(rows_path), artifact_path="data")
        return active.info.run_id
    finally:
        mlflow.end_run()


def log_eval_metrics(
    metrics: dict[str, float],
    *,
    params: dict[str, Any] | None = None,
    artifact_paths: list[Path] | None = None,
    experiment_name: str | None = None,
    run_name: str | None = None,
) -> str | None:
    import mlflow

    name = experiment_name or os.environ.get(
        "LIDAR_MLFLOW_EVAL_EXPERIMENT",
        os.environ.get("MLFLOW_EXPERIMENT_NAME", DEFAULT_EXPERIMENT) + "-eval",
    )
    configure_mlflow(name)
    active = mlflow.start_run(run_name=run_name or "holdout-replay")
    try:
        mlflow.set_tag("eval_type", "holdout_replay")
        if params:
            mlflow.log_params({k: str(v) for k, v in params.items()})
        for key, value in metrics.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                mlflow.log_metric(key, float(value))
        for path in artifact_paths or []:
            if path.is_file():
                mlflow.log_artifact(str(path))
        return active.info.run_id
    finally:
        mlflow.end_run()
