# Legacy bundle — prefer project-root predict.py for CML Models UI.
# Kept for model-deployment/ folder builds; mirrors root predict.py API.

from __future__ import annotations

import base64
import json
import os
from pathlib import Path

import joblib
import numpy as np

from features import extract_features_from_las_bytes

_MODEL = joblib.load(Path(__file__).resolve().parent / "model.joblib")


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


def predict(args: dict) -> dict:
    if "las_bytes_base64" in args:
        payload = base64.b64decode(args["las_bytes_base64"])
    elif "s3_uri" in args:
        uri = args["s3_uri"].replace("s3a://", "s3://")
        bucket, key = uri[5:].split("/", 1)
        payload = _get_s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()
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
    }
