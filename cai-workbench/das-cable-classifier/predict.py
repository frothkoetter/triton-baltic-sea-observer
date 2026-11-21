# CML Model — input function ``predict``
import base64
import os
import sys

_root = os.environ.get("CDSW_WORKING_DIRECTORY") or os.getcwd()
if _root not in sys.path:
    sys.path.insert(0, _root)

import joblib
import numpy as np
import cml.models_v1 as models

from src.features import features_from_hdf5_bytes, features_from_summary
from src.labels import FEATURE_NAMES

_MODEL = joblib.load(os.path.join(_root, "model.pkl"))


def _rows_from_args(args: dict) -> list[dict]:
    if "hdf5_bytes_base64" in args:
        raw = base64.b64decode(args["hdf5_bytes_base64"])
        return features_from_hdf5_bytes(raw)
    if "features" in args and isinstance(args["features"], dict):
        return [args["features"]]
    raise ValueError("Provide hdf5_bytes_base64 (NiFi DAS output) or features dict")


@models.cml_model
def predict(args: dict) -> dict:
    rows = _rows_from_args(args)
    clf = _MODEL.named_steps["clf"]
    results = []
    for row in rows:
        gt = row.get("threat_class") or row.get("threat_class_gt")
        summary = {k: row[k] for k in FEATURE_NAMES if k in row}
        vec = features_from_summary(summary).reshape(1, -1)
        proba = clf.predict_proba(vec)[0]
        classes = list(clf.classes_)
        idx = int(np.argmax(proba))
        results.append(
            {
                "segment": row.get("segment"),
                "threat_type": classes[idx],
                "confidence": float(proba[idx]),
                "probabilities": {c: float(p) for c, p in zip(classes, proba)},
                "ground_truth": gt,
                "chainage_km": summary.get("chainage_km"),
            }
        )
    if len(results) == 1:
        return results[0]
    return {"predictions": results, "count": len(results)}