# Deploy with entry function ``predict_array`` when the endpoint must return only a JSON array.
import os
import sys

_root = os.environ.get("CDSW_WORKING_DIRECTORY") or os.getcwd()
if _root not in sys.path:
    sys.path.insert(0, _root)

import cml.models_v1 as models

from src.detect import detect_outliers


@models.cml_model
def predict_array(args: dict) -> list:
    return detect_outliers(args)
