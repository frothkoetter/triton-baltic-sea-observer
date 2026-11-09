# Example: call deployed model, read metrics, track delayed / aggregate metrics.
# Set dev=True for local predict_with_metrics.predict; False when calling a deployment.

import json
import os
import time
from pathlib import Path

import cml.metrics_v1 as metrics
import cml.models_v1 as models

dev = True

if dev:
    from predict_with_metrics import predict

if dev:
    model_deployment_crn = metrics.dev_model_deployment_crn
    model_access_key = None
else:
    model_deployment_crn = None
    if model_deployment_crn is None:
        raise ValueError("Please set a valid model deployment CRN")

    model_access_key = None
    if model_access_key is None:
        raise ValueError("Please set the model's access key")

    user_api_key = None

_root = Path(os.environ.get("CDSW_WORKING_DIRECTORY", os.getcwd()))
_rows_path = _root / "artifacts" / "training_rows.csv"
sample_uris: list[str] = []
if _rows_path.is_file():
    import csv

    with _rows_path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            uri = (row.get("s3_uri") or "").strip()
            if uri.startswith("s3://"):
                sample_uris.append(uri)
    sample_uris = sample_uris[:5]

if not sample_uris:
    prefix = os.environ.get(
        "LIDAR_S3_PREFIX",
        "s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/",
    )
    sample_uris = [prefix.rstrip("/") + "/"]

start_timestamp_ms = int(round(time.time() * 1000))

uuids = []
predictions = []
for uri in sample_uris:
    args = {"s3_uri": uri}
    if model_access_key is not None:
        output = models.call_model(
            model_access_key, args, api_key=user_api_key
        )["response"]
    else:
        output = predict(args)
    uuids.append(output["uuid"])
    predictions.append(output.get("vessel_type") or output.get("prediction"))

end_timestamp_ms = int(round(time.time() * 1000))

data = metrics.read_metrics(
    model_deployment_crn=model_deployment_crn,
    start_timestamp_ms=start_timestamp_ms,
    end_timestamp_ms=end_timestamp_ms,
    dev=dev,
)
print("metrics rows:", len(data.get("metrics", [])))

# Delayed metrics: e.g. human-reviewed vessel type (demo uses predicted type as placeholder).
for i, uuid in enumerate(uuids):
    metrics.track_delayed_metrics(
        {"reviewed_vessel_type": predictions[i]},
        uuid,
        dev=dev,
    )

if predictions:
    metrics.track_aggregate_metrics(
        {"n_predictions": float(len(predictions))},
        start_timestamp_ms,
        end_timestamp_ms,
        model_deployment_crn=model_deployment_crn,
        dev=dev,
    )

if os.environ.get("LIDAR_MLFLOW_EVAL", "").lower() in ("1", "true", "yes"):
    from src.eval_metrics import evaluate_saved_model
    from src.mlflow_tracking import log_eval_metrics, mlflow_enabled

    _model = _root / "model.pkl"
    _rows = _root / "artifacts" / "training_rows.csv"
    if mlflow_enabled() and _model.is_file() and _rows.is_file():
        eval_result = evaluate_saved_model(_model, _rows, out_dir=_root / "artifacts")
        run_id = log_eval_metrics(
            eval_result["metrics"],
            params=eval_result["params"],
            artifact_paths=[Path(p) for p in eval_result["artifact_paths"]],
        )
        print("MLflow eval run id:", run_id)
        print("replay accuracy:", eval_result["metrics"].get("accuracy"))
    else:
        print("LIDAR_MLFLOW_EVAL set but model, rows, or mlflow missing — skip replay eval.")
