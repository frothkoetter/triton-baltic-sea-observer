# CML Model deployment — entry function ``classify`` (S3 LAS input).
import os
import sys

_root = os.environ.get("CDSW_WORKING_DIRECTORY") or os.getcwd()
os.chdir(_root)
if _root not in sys.path:
    sys.path.insert(0, _root)

import joblib
import numpy as np
import cml.models_v1 as models

from src.features import extract_features_from_las_bytes
from src.s3_client import get_s3_client

_MODEL = joblib.load(os.path.join(_root, "model.pkl"))
_S3 = get_s3_client()


def _las_bytes_from_s3_uri(s3_uri: str) -> bytes:
    uri = s3_uri.replace("s3a://", "s3://")
    if not uri.startswith("s3://"):
        raise ValueError("s3_uri must start with s3:// or s3a://")
    bucket, key = uri[5:].split("/", 1)
    return _S3.get_object(Bucket=bucket, Key=key)["Body"].read()


@models.cml_model
def classify(args: dict) -> dict:
    payload = _las_bytes_from_s3_uri(args["s3_uri"])
    vec = extract_features_from_las_bytes(payload).reshape(1, -1)
    proba = _MODEL.predict_proba(vec)[0]
    classes = list(_MODEL.classes_)
    idx = int(np.argmax(proba))
    return {
        "vessel_type": str(classes[idx]),
        "confidence": float(proba[idx]),
    }
