# CML model with per-prediction metrics (for use_model_metrics.py in dev mode).
import base64
import os
import sys

_root = os.environ.get("CDSW_WORKING_DIRECTORY") or os.getcwd()
if _root not in sys.path:
    sys.path.insert(0, _root)

import joblib
import numpy as np
import cml.models_v1 as models

from src.features import extract_features_from_las_bytes, ground_truth_label_from_las_bytes
from src.s3_client import get_s3_client

_MODEL = joblib.load(os.path.join(_root, "model.pkl"))


def _read_las_bytes(args: dict) -> bytes:
    if "las_bytes_base64" in args:
        return base64.b64decode(args["las_bytes_base64"])
    if "s3_uri" in args:
        uri = args["s3_uri"].replace("s3a://", "s3://")
        if not uri.startswith("s3://"):
            raise ValueError("s3_uri must start with s3:// or s3a://")
        bucket, key = uri[5:].split("/", 1)
        client = get_s3_client()
        return client.get_object(Bucket=bucket, Key=key)["Body"].read()
    raise ValueError("Provide las_bytes_base64 or s3_uri")


@models.cml_model
def predict(args: dict) -> dict:
    payload = _read_las_bytes(args)
    vec = extract_features_from_las_bytes(payload).reshape(1, -1)
    clf = _MODEL.named_steps["clf"]
    proba = clf.predict_proba(vec)[0]
    classes = list(clf.classes_)
    idx = int(np.argmax(proba))
    vessel_type = str(classes[idx])
    confidence = float(proba[idx])

    metrics = {
        "confidence": confidence,
        "vessel_type": vessel_type,
    }
    truth = ground_truth_label_from_las_bytes(payload)
    if truth:
        metrics["ground_truth_label"] = truth
        metrics["label_match"] = float(truth == vessel_type)

    return models.wrap_prediction(
        {
            "vessel_type": vessel_type,
            "confidence": confidence,
        },
        metrics=metrics,
    )
