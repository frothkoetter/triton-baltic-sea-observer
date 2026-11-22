# CML Model — input function ``predict``
from __future__ import annotations

import base64
import os
import sys
from pathlib import Path


def _model_pkl_path() -> Path:
    override = os.environ.get("DAS_MODEL_PKL", "").strip()
    if override:
        p = Path(override)
        if p.is_file():
            return p
        raise FileNotFoundError(f"DAS_MODEL_PKL not found: {p}")

    tried: list[str] = []
    roots: list[Path] = []
    here = Path(__file__).resolve().parent
    roots.append(here)

    work = os.environ.get("CDSW_WORKING_DIRECTORY", "").strip()
    if work and work not in ("", "/"):
        base = Path(work)
        roots.extend(
            [
                base,
                base / "cai-workbench" / "das-cable-classifier",
                base / "das-cable-classifier",
            ]
        )
    home = Path.home()
    roots.extend(
        [
            home / "cai-workbench" / "das-cable-classifier",
            Path("/home/cdsw") / "cai-workbench" / "das-cable-classifier",
            Path.cwd(),
        ]
    )

    seen: set[Path] = set()
    for root in roots:
        try:
            r = root.resolve()
        except OSError:
            continue
        if r in seen:
            continue
        seen.add(r)
        for rel in ("model.pkl", "bundled/model.pkl"):
            p = r / rel
            tried.append(str(p))
            if p.is_file():
                if str(r) not in sys.path:
                    sys.path.insert(0, str(r))
                print(f"das-cable-classifier: loading {p}", file=sys.stderr, flush=True)
                return p

    raise FileNotFoundError(
        "model.pkl missing in model runtime (checked: "
        + ", ".join(tried[:12])
        + (f", … +{len(tried) - 12} more" if len(tried) > 12 else "")
        + "). Run cdsw-build.sh (copies bundled/model.pkl), git pull for predict.py fix, "
        f"or export DAS_MODEL_PKL. CDSW_WORKING_DIRECTORY={work or '(unset)'}"
    )


_MODEL_PKL = _model_pkl_path()
_root = _MODEL_PKL.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import joblib
import numpy as np
import cml.models_v1 as models

from src.features import features_from_hdf5_bytes, features_from_summary
from src.labels import FEATURE_NAMES

_MODEL = joblib.load(_MODEL_PKL)


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