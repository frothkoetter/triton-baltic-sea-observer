#!/usr/bin/env python3
"""
Train a vessel-type classifier from LiDAR (.las) files in S3.

Usage:
  python3 train.py --s3-prefix "s3://bucket/path/"
  python3 train.py --local-dir /path/to/las/files

Cloudera AI Python (Cell In[n]): do NOT use Path(__file__). Run session_train.py
or: import train; train.main()  (see RUNTIME.md).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from cml_bootstrap import ensure_project_on_sys_path

ensure_project_on_sys_path()

from project_root import resolve_project_root

ROOT = resolve_project_root()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_TRAIN_DEPS = ("joblib", "numpy", "pandas", "sklearn", "laspy", "boto3")


def _requirements_file() -> Path:
    train_req = ROOT / "requirements-train.txt"
    return train_req if train_req.is_file() else ROOT / "requirements.txt"


def ensure_runtime_dependencies() -> None:
    if os.environ.get("LIDAR_SKIP_PIP_INSTALL", "").lower() in ("1", "true", "yes"):
        return
    missing = [name for name in _TRAIN_DEPS if not _can_import(name)]
    if not missing:
        return
    req = _requirements_file()
    if not req.is_file():
        raise ModuleNotFoundError(
            f"Missing packages: {', '.join(missing)}. "
            f"Run: python3 -m pip install -r {req}"
        )
    print(f"Installing from {req} ({', '.join(missing)}) …")
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--user", "-r", str(req)],
    )


def _can_import(name: str) -> bool:
    try:
        __import__(name)
        return True
    except ImportError:
        return False


ensure_runtime_dependencies()

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import StratifiedKFold, cross_val_predict
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.features import extract_features_from_las_bytes, ground_truth_label_from_las_bytes
from src.labels import FEATURE_NAMES
from src.s3_las_loader import iter_las_objects, label_from_key

DEFAULT_S3_PREFIX = "s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/"
ARTIFACT_DIR = ROOT / "artifacts"
DEPLOY_DIR = ROOT / "model-deployment"


def load_training_table_from_dir(local_dir: Path, max_files: int | None) -> pd.DataFrame:
    from src.s3_las_loader import is_las_bytes

    rows: list[dict] = []
    candidates: list[Path] = []
    for path in sorted(local_dir.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix.lower() == ".las" or path.suffix == "":
            candidates.append(path)
    for i, path in enumerate(candidates):
        if max_files is not None and i >= max_files:
            break
        body = path.read_bytes()
        if not is_las_bytes(body):
            print(f"skip (not LAS): {path}")
            continue
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
        raise RuntimeError(f"No labeled LAS files under {local_dir}")
    return pd.DataFrame(rows)


def load_training_table(
    s3_prefix: str,
    max_files: int | None,
    *,
    require_las_magic: bool = True,
) -> pd.DataFrame:
    rows: list[dict] = []
    for i, obj in enumerate(
        iter_las_objects(s3_prefix, require_las_magic=require_las_magic)
    ):
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
            f"No labeled LAS objects under {s3_prefix} "
            "(.las or extensionless keys with LASF header)."
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

    pipeline.fit(X, y)
    if n_splits < 2:
        print("Training set fit (no cross-validation):")
        print(classification_report(y, pipeline.predict(X)))

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, ARTIFACT_DIR / "model.joblib")
    labels = sorted(df["label"].unique().tolist())
    with open(ARTIFACT_DIR / "label_names.json", "w", encoding="utf-8") as f:
        json.dump(labels, f, indent=2)
    with open(ARTIFACT_DIR / "training_meta.json", "w", encoding="utf-8") as f:
        json.dump(
            {"n_samples": len(df), "labels": labels, "feature_names": FEATURE_NAMES},
            f,
            indent=2,
        )
    df.to_csv(ARTIFACT_DIR / "training_rows.csv", index=False)
    joblib.dump(pipeline, ROOT / "model.pkl")
    with open(ROOT / "label_names.json", "w", encoding="utf-8") as f:
        json.dump(labels, f, indent=2)
    print(f"Wrote model to {ARTIFACT_DIR / 'model.joblib'} and {ROOT / 'model.pkl'}")
    return pipeline


def package_deployment() -> None:
    import shutil

    DEPLOY_DIR.mkdir(parents=True, exist_ok=True)
    for name in ("model.joblib", "label_names.json", "training_meta.json"):
        shutil.copy2(ARTIFACT_DIR / name, DEPLOY_DIR / name)
    if (ROOT / "model.pkl").is_file():
        shutil.copy2(ROOT / "model.pkl", DEPLOY_DIR / "model.pkl")
    for mod in ("features.py", "labels.py"):
        shutil.copy2(ROOT / "src" / mod, DEPLOY_DIR / mod)
    print(f"Packaged deployment dir: {DEPLOY_DIR}")


def _filter_kernel_argv(argv: list[str]) -> list[str]:
    """IPython/Jupyter passes ``-f /path/kernel-….json``; ignore for argparse."""
    cleaned: list[str] = []
    skip_next = False
    for arg in argv:
        if skip_next:
            skip_next = False
            continue
        if arg in ("-f", "--f"):
            skip_next = True
            continue
        if "kernel-" in arg and arg.endswith(".json"):
            continue
        cleaned.append(arg)
    return cleaned


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Train LiDAR vessel classifier from LAS files (S3 or local directory)."
    )
    parser.add_argument(
        "--s3-prefix",
        default=os.environ.get("LIDAR_S3_PREFIX", DEFAULT_S3_PREFIX),
        help="S3 prefix with training LAS objects",
    )
    parser.add_argument(
        "--local-dir",
        default=os.environ.get("LIDAR_LOCAL_DIR"),
        help="Local directory (overrides --s3-prefix)",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=int(os.environ["LIDAR_MAX_FILES"]) if os.environ.get("LIDAR_MAX_FILES") else None,
        help="Limit number of files loaded",
    )
    parser.add_argument(
        "--no-las-magic-check",
        action="store_true",
        help="Skip LASF header check for extensionless S3 keys",
    )
    cli = _filter_kernel_argv(argv if argv is not None else sys.argv[1:])
    args = parser.parse_args(cli)

    if args.local_dir:
        print(f"Training from local dir: {args.local_dir}")
        df = load_training_table_from_dir(Path(args.local_dir), args.max_files)
    else:
        print(f"Training from S3 prefix: {args.s3_prefix}")
        df = load_training_table(
            args.s3_prefix,
            args.max_files,
            require_las_magic=not args.no_las_magic_check,
        )
    train_and_save(df)
    package_deployment()


if __name__ == "__main__":
    main()
