# CML Model deployment — entry function ``detect_outliers``.
# Input: {"records": [ ... AIS rows ... ]} or {"ndjson": "..."}
# Output: list of outlier dicts (Reason not null only).
import json
import os
import sys

_root = os.environ.get("CDSW_WORKING_DIRECTORY") or os.getcwd()
if _root not in sys.path:
    sys.path.insert(0, _root)

import cml.models_v1 as models

from src.detect import detect_outliers as run_detect_outliers


@models.cml_model
def detect_outliers(args: dict) -> list:
    return run_detect_outliers(args)


@models.cml_model
def predict(args: dict) -> dict:
    """Alias for tooling that expects ``predict``; body is JSON array string."""
    outliers = run_detect_outliers(args)
    return {"body": json.dumps(outliers, ensure_ascii=False)}
