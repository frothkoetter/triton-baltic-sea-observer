"""
CML Model Deployment entrypoint.

Request JSON (any of):
  - {"s3_uri": "s3://bucket/path/file.las"}  (needs S3 credentials on deployment)
  - {"las_bytes_base64": "<base64>"}

Response:
  {"vessel_type": "corvette", "confidence": 0.92, "probabilities": {...}}
"""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path

import joblib
import numpy as np

from features import extract_features_from_las_bytes
from labels import FEATURE_NAMES

_MODEL = None
_LABELS: list[str] | None = None


def _load():
    global _MODEL, _LABELS
    if _MODEL is not None:
        return
    base = Path(__file__).resolve().parent
    _MODEL = joblib.load(base / "model.joblib")
    with open(base / "label_names.json", encoding="utf-8") as f:
        _LABELS = json.load(f)


def _get_s3_client():
    connection_name = os.environ.get("LIDAR_S3_CONNECTION_NAME", "S3 Object Store")
    mode = os.environ.get("LIDAR_USE_CML_S3", "auto").lower()
    if mode in ("1", "true", "yes", "auto"):
        try:
            import cml.data_v1 as cmldata

            return cmldata.get_connection(connection_name).get_base_connection()
        except Exception:
            if mode in ("1", "true", "yes"):
                raise
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-central-1"),
    )


def _read_las_from_s3(s3_uri: str) -> bytes:
    uri = s3_uri.replace("s3a://", "s3://")
    if not uri.startswith("s3://"):
        raise ValueError("s3_uri must start with s3:// or s3a://")
    path = uri[5:]
    bucket, key = path.split("/", 1)
    client = _get_s3_client()
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


def predict(args: dict) -> dict:
    _load()
    if "las_bytes_base64" in args:
        payload = base64.b64decode(args["las_bytes_base64"])
    elif "s3_uri" in args:
        payload = _read_las_from_s3(args["s3_uri"])
    else:
        raise ValueError("Provide las_bytes_base64 or s3_uri")

    vec = extract_features_from_las_bytes(payload).reshape(1, -1)
    clf = _MODEL.named_steps["clf"]
    proba = clf.predict_proba(vec)[0]
    classes = list(clf.classes_)
    idx = int(np.argmax(proba))
    return {
        "vessel_type": classes[idx],
        "confidence": float(proba[idx]),
        "probabilities": {c: float(p) for c, p in zip(classes, proba)},
        "feature_names": FEATURE_NAMES,
    }


# CDSW / CML may call predict with a JSON string
def predict_json(input_json: str) -> str:
    result = predict(json.loads(input_json))
    return json.dumps(result)
