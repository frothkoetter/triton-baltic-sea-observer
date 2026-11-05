"""Train IsolationForest on synthetic normal AIS traffic (severity calibration)."""

from __future__ import annotations

import json
import random
from pathlib import Path

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from src.features import batch_feature_matrix


def _synthetic_normal_batch(n_ships: int = 40, points: int = 3) -> list[dict]:
    rows = []
    base_ts = "2026-10-28 12:00:00.000"
    for i in range(n_ships):
        mmsi = 123456000 + i
        lat = 54.0 + random.random() * 6.0
        lon = 10.0 + random.random() * 18.0
        status = random.choice(["Underway using engine", "Underway", "Moored"])
        speed = random.uniform(0.5, 12.0) if "Underway" in status else random.uniform(0, 1.5)
        course = random.uniform(0, 359.9)
        dest = random.choice(["Rostock", "Helsinki", "Riga", "Tallinn"])
        for p in range(points):
            rows.append(
                {
                    "MMSI": mmsi,
                    "Event_Timestamp": base_ts,
                    "Latitude": lat + p * 0.01,
                    "Longitude": lon + p * 0.015,
                    "Speed": min(12.0, speed + random.uniform(-0.5, 0.5)),
                    "Course": (course + p * 2) % 360,
                    "Status": status,
                    "Destination": dest,
                }
            )
    return rows


def train_and_save(model_path: Path | None = None, n_batches: int = 200) -> Path:
    model_path = model_path or Path("model.pkl")
    rows: list[dict] = []
    for _ in range(n_batches):
        rows.extend(_synthetic_normal_batch())

    X = batch_feature_matrix(rows)
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)
    clf = IsolationForest(
        n_estimators=128,
        contamination=0.05,
        random_state=42,
    )
    clf.fit(Xs)
    joblib.dump({"scaler": scaler, "clf": clf}, model_path)
    meta = {"n_samples": int(X.shape[0]), "n_features": int(X.shape[1])}
    Path("artifacts").mkdir(exist_ok=True)
    Path("artifacts/training_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    return model_path


def main() -> None:
    path = train_and_save()
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
