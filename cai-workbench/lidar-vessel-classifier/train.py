#!/usr/bin/env python3
"""
Train a vessel-type classifier from LiDAR (.las) files in S3.

Run in Cloudera AI Workbench (session or job). Set LIDAR_S3_PREFIX to your bucket path.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import cross_val_predict, StratifiedKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


from setup_project import ensure_src_path, project_root

ROOT = ensure_src_path()

from features import extract_features_from_las_bytes, ground_truth_label_from_las_bytes
from labels import FEATURE_NAMES
from s3_las_loader import iter_las_objects, label_from_key

DEFAULT_S3_PREFIX = (
    "s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/"
)
ARTIFACT_DIR = ROOT / "artifacts"
DEPLOY_DIR = ROOT / "model-deployment"


def load_training_table_from_dir(local_dir: Path, max_files: int | None) -> pd.DataFrame:
    rows: list[dict] = []
    paths = sorted(local_dir.glob("**/*.las"))
    for i, path in enumerate(paths):
        if max_files is not None and i >= max_files:
            break
        body = path.read_bytes()
        features = extract_features_from_las_bytes(body)
        label = ground_truth_label_from_las_bytes(body) or label_from_key(path.name)
        if not label:
            print(f"skip (no label): {path}")
            continue
        row = {name: float(val) for name, val in zip(FEATURE_NAMES, features)}
        row["label"] = label
        row["s3_uri"] = str(path)
        rows.append(row)
        print(f"loaded {path} → {label}")
    if not rows:
        raise RuntimeError(f"No labeled .las under {local_dir}")
    return pd.DataFrame(rows)


def load_training_table(s3_prefix: str, max_files: int | None) -> pd.DataFrame:
    rows: list[dict] = []
    for i, obj in enumerate(iter_las_objects(s3_prefix)):
        if max_files is not None and i >= max_files:
            break
        features = extract_features_from_las_bytes(obj.body)
        label = ground_truth_label_from_las_bytes(obj.body) or label_from_key(obj.key)
        if not label:
            print(f"skip (no label): {obj.s3_uri}")
            continue
        row = {name: float(val) for name, val in zip(FEATURE_NAMES, features)}
        row["label"] = label
        row["s3_uri"] = obj.s3_uri
        rows.append(row)
        print(f"loaded {obj.s3_uri} → {label} ({int(row['n_points'])} pts)")

    if not rows:
        raise RuntimeError(
            f"No labeled .las files under {s3_prefix}. "
            "Upload simulator samples or ensure classification codes 1–9 are set."
        )
    return pd.DataFrame(rows)


def train_and_save(df: pd.DataFrame) -> Pipeline:
    X = df[FEATURE_NAMES].values
    y = df["label"].values

    _, counts = np.unique(y, return_counts=True)
    min_class_count = int(counts.min()) if len(counts) else 0
    n_splits = min(5, min_class_count, len(y))
    if n_splits < 2:
        print("Skipping cross-validation (need ≥2 samples per class for CV).")

    pipeline = Pipeline(
        steps=[
            ("scale", StandardScaler()),
            (
                "clf",
                RandomForestClassifier(
                    n_estimators=200,
                    max_depth=12,
                    class_weight="balanced",
                    random_state=42,
                    n_jobs=-1,
                ),
            ),
        ]
    )

    if n_splits >= 2:
        cv = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
        y_pred = cross_val_predict(pipeline, X, y, cv=cv)
        print(classification_report(y, y_pred))
    else:
        print(classification_report(y, pipeline.predict(X)))

    pipeline.fit(X, y)

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, ARTIFACT_DIR / "model.joblib")
    labels = sorted(df["label"].unique().tolist())
    with open(ARTIFACT_DIR / "label_names.json", "w", encoding="utf-8") as f:
        json.dump(labels, f, indent=2)
    with open(ARTIFACT_DIR / "training_meta.json", "w", encoding="utf-8") as f:
        json.dump(
            {
                "n_samples": len(df),
                "labels": labels,
                "feature_names": FEATURE_NAMES,
            },
            f,
            indent=2,
        )
    df.to_csv(ARTIFACT_DIR / "training_rows.csv", index=False)
    print(f"Wrote model to {ARTIFACT_DIR / 'model.joblib'}")
    return pipeline


def package_deployment() -> None:
    """Copy runtime files for CML Model Deployment."""
    import shutil

    DEPLOY_DIR.mkdir(parents=True, exist_ok=True)
    for name in ("model.joblib", "label_names.json", "training_meta.json"):
        shutil.copy2(ARTIFACT_DIR / name, DEPLOY_DIR / name)
    for mod in ("features.py", "labels.py"):
        shutil.copy2(ROOT / "src" / mod, DEPLOY_DIR / mod)
    print(f"Packaged deployment dir: {DEPLOY_DIR}")


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Train LiDAR vessel classifier from .las files (S3 or local directory)."
    )
    parser.add_argument(
        "--s3-prefix",
        default=os.environ.get("LIDAR_S3_PREFIX", DEFAULT_S3_PREFIX),
        help="S3 prefix with training .las files (s3:// or s3a://)",
    )
    parser.add_argument(
        "--local-dir",
        default=os.environ.get("LIDAR_LOCAL_DIR"),
        help="Local directory of .las files (overrides --s3-prefix)",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=int(os.environ["LIDAR_MAX_FILES"]) if os.environ.get("LIDAR_MAX_FILES") else None,
        help="Limit number of files loaded",
    )
    args = parser.parse_args(argv)

    if args.local_dir:
        print(f"Training from local dir: {args.local_dir}")
        df = load_training_table_from_dir(Path(args.local_dir), args.max_files)
    else:
        print(f"Training from S3 prefix: {args.s3_prefix}")
        df = load_training_table(args.s3_prefix, args.max_files)
    train_and_save(df)
    package_deployment()


if __name__ == "__main__":
    main()
