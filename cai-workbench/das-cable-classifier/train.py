#!/usr/bin/env python3
"""Train DAS cable threat classifier from synthetic spectra (NiFi-compatible engine)."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[1]  # repo root (cai-workbench parent)
NIFI_UPLOAD = REPO / "nifi-processors" / "upload"
if NIFI_UPLOAD.as_posix() not in sys.path:
    sys.path.insert(0, NIFI_UPLOAD.as_posix())

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, f1_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from das_acoustic_engine import THREAT_CLASSES, generate_das_batch  # noqa: E402

from src.labels import FEATURE_NAMES  # noqa: E402

ARTIFACTS = ROOT / "artifacts"
BUNDLED = ROOT / "bundled"


def build_training_frame(n_batches: int, seed: int, cable: str) -> pd.DataFrame:
    rows = []
    for b in range(n_batches):
        segments, _meta = generate_das_batch(cable, n_segments=8, seed=seed + b)
        for seg in segments:
            feats = seg.summary_features()
            rows.append({**feats, "threat_class": seg.threat_class})
    return pd.DataFrame(rows)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--cable", default="BALTICCONNECTOR")
    p.add_argument("--batches", type=int, default=80)
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args(argv)

    df = build_training_frame(args.batches, args.seed, args.cable)
    X = df[FEATURE_NAMES].values
    y = df["threat_class"].values
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=args.seed, stratify=y
    )

    pipe = Pipeline(
        [
            ("scale", StandardScaler()),
            ("clf", RandomForestClassifier(n_estimators=200, max_depth=12, random_state=args.seed)),
        ]
    )
    pipe.fit(X_train, y_train)
    pred = pipe.predict(X_test)
    acc = accuracy_score(y_test, pred)
    macro_f1 = f1_score(y_test, pred, average="macro")

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    BUNDLED.mkdir(parents=True, exist_ok=True)
    model_path = ARTIFACTS / "model.joblib"
    joblib.dump(pipe, model_path)
    joblib.dump(pipe, ROOT / "model.pkl")
    joblib.dump(pipe, BUNDLED / "model.pkl")

    labels = sorted(df["threat_class"].unique().tolist())
    with open(ARTIFACTS / "label_names.json", "w", encoding="utf-8") as f:
        json.dump(labels, f, indent=2)
    with open(BUNDLED / "label_names.json", "w", encoding="utf-8") as f:
        json.dump(labels, f, indent=2)

    meta = {
        "cable_asset": args.cable,
        "n_samples": int(len(df)),
        "labels": labels,
        "threat_classes_all": list(THREAT_CLASSES),
        "metrics": {"accuracy": acc, "macro_f1": macro_f1},
        "report": classification_report(y_test, pred),
        "feature_names": FEATURE_NAMES,
        "schema": "triton/das-cable/v1",
    }
    (ARTIFACTS / "training_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    df.to_csv(ARTIFACTS / "training_rows.csv", index=False)
    print(f"Trained on {len(df)} segments | accuracy={acc:.3f} macro_f1={macro_f1:.3f}")
    print(meta["report"])


if __name__ == "__main__":
    main()