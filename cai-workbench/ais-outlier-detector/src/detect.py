"""Orchestrate rules + ML severity for AIS outlier detection."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import joblib
import numpy as np

from src.features import batch_feature_matrix
from src.normalize import format_outlier, normalize_record, parse_input_payload
from src.rules import detect_rule_outliers

_MODEL: Any | None = None


def _load_model() -> Any | None:
    global _MODEL
    if _MODEL is not None:
        return _MODEL
    root = Path(os.environ.get("CDSW_WORKING_DIRECTORY", os.getcwd()))
    path = root / "model.pkl"
    if path.is_file():
        _MODEL = joblib.load(path)
    else:
        _MODEL = None
    return _MODEL


def _apply_ml_severity(records: List[Dict[str, Any]], outliers: List[Dict[str, Any]]) -> None:
    model = _load_model()
    if model is None or not records or not outliers:
        return
    X = batch_feature_matrix(records)
    if X.shape[0] == 0:
        return
    scaler = model["scaler"]
    clf = model["clf"]
    Xs = scaler.transform(X)
    scores = -clf.decision_function(Xs)
    score_by_idx = {i: float(scores[i]) for i in range(len(records))}

    record_index = {
        (int(r["MMSI"]), str(r["Event_Timestamp"])): i for i, r in enumerate(records)
    }
    for out in outliers:
        idx = record_index.get((int(out["MMSI"]), str(out["Event_Timestamp"])))
        if idx is None:
            continue
        raw = score_by_idx.get(idx, 0.0)
        bump = int(min(25, max(0, raw * 12)))
        out["Severity_Level"] = int(min(100, max(0, int(out["Severity_Level"]) + bump)))


def detect_outliers_from_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized = [normalize_record(r) if "MMSI" in r else r for r in records]
    rule_hits = detect_rule_outliers(normalized)
    _apply_ml_severity(normalized, rule_hits)
    return [format_outlier(r) for r in rule_hits]


def detect_outliers(payload: Any) -> List[Dict[str, Any]]:
    records = parse_input_payload(payload)
    return detect_outliers_from_records(records)
