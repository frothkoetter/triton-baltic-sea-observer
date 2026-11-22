#!/usr/bin/env python3
"""Train a small default model when bundled/ is not in the CML model build context."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.labels import FEATURE_NAMES, THREAT_CLASSES

SEED = 42
SAMPLES_PER_CLASS = 80


def _synthetic_rows() -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(SEED)
    rows: list[list[float]] = []
    labels: list[str] = []
    for i, cls in enumerate(THREAT_CLASSES):
        for _ in range(SAMPLES_PER_CLASS):
            base = rng.uniform(0.05, 0.35, size=len(FEATURE_NAMES))
            base[0] += 0.1 * (i % 4)
            base[4] += 5.0 * (i + 1)
            base[6] += 3.0 * (i + 1)
            base[10] += 0.2 * i
            rows.append(base.tolist())
            labels.append(cls)
    return np.asarray(rows, dtype=np.float64), np.asarray(labels)


def main() -> None:
    X, y = _synthetic_rows()
    pipe = Pipeline(
        [
            ("scale", StandardScaler()),
            ("clf", RandomForestClassifier(n_estimators=64, max_depth=10, random_state=SEED)),
        ]
    )
    pipe.fit(X, y)
    out = ROOT / "model.pkl"
    joblib.dump(pipe, out)
    labels_path = ROOT / "label_names.json"
    labels_path.write_text(json.dumps(sorted(THREAT_CLASSES), indent=2), encoding="utf-8")
    print(f"Wrote {out} ({out.stat().st_size} bytes) from synthetic deploy bootstrap")


if __name__ == "__main__":
    main()