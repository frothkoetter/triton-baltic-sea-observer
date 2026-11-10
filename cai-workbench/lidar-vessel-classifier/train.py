#!/usr/bin/env python3
"""Train vessel-type classifier from LiDAR (.las) in S3 or a local directory."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def _root() -> Path:
    try:
        here = Path(__file__).resolve().parent
        if (here / "fit.py").is_file():
            return here
    except NameError:
        pass
    return Path(os.environ.get("CDSW_WORKING_DIRECTORY", os.getcwd())).resolve()


ROOT = _root()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_TRAIN_DEPS = ("joblib", "numpy", "pandas", "sklearn", "laspy", "boto3")


def _ensure_deps() -> None:
    if os.environ.get("LIDAR_SKIP_PIP_INSTALL", "").lower() in ("1", "true", "yes"):
        return
    missing = [n for n in _TRAIN_DEPS if not _can_import(n)]
    if not missing:
        return
    req = ROOT / "requirements.txt"
    print(f"Installing from {req} ({', '.join(missing)}) …")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--user", "-r", str(req)])


def _can_import(name: str) -> bool:
    try:
        __import__(name)
        return True
    except ImportError:
        return False


_ensure_deps()

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
)
from sklearn.model_selection import StratifiedKFold, cross_val_predict, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.features import extract_features_from_las_bytes, ground_truth_label_from_las_bytes
from src.labels import FEATURE_NAMES
from src.s3_las_loader import iter_las_objects, label_from_key

DEFAULT_S3_PREFIX = "s3://cdwh-buk-7dbf40cb/data/user/frothkoetter/geospatial/"
ARTIFACT_DIR = ROOT / "artifacts"


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


RF_PARAMS = {
    "n_estimators": 200,
    "max_depth": 12,
    "class_weight": "balanced",
    "random_state": 42,
    "n_jobs": -1,
}


def train_and_save(df: pd.DataFrame) -> tuple[Pipeline, dict]:
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
            ("clf", RandomForestClassifier(**RF_PARAMS)),
        ]
    )

    labels = sorted(df["label"].unique().tolist())
    eval_method = "train_fit"
    y_eval = y
    y_pred = None

    if n_splits >= 2:
        eval_method = "stratified_kfold_cv"
        cv = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
        y_pred = cross_val_predict(pipeline, X, y, cv=cv)
        print(classification_report(y, y_pred, labels=labels, zero_division=0))
    else:
        test_size = 0.2 if len(y) >= 5 else 0.0
        if test_size > 0 and min_class_count >= 2:
            eval_method = "holdout_split"
            X_tr, X_va, y_tr, y_va = train_test_split(
                X,
                y,
                test_size=test_size,
                random_state=42,
                stratify=y,
            )
            holdout = Pipeline(
                steps=[
                    ("scale", StandardScaler()),
                    ("clf", RandomForestClassifier(**RF_PARAMS)),
                ]
            )
            holdout.fit(X_tr, y_tr)
            y_pred = holdout.predict(X_va)
            y_eval = y_va
            print(classification_report(y_va, y_pred, labels=labels, zero_division=0))

    pipeline.fit(X, y)
    if y_pred is None:
        print("Training set fit (no held-out evaluation):")
        y_pred = pipeline.predict(X)
        y_eval = y
        print(classification_report(y, y_pred, labels=labels, zero_division=0))

    report_dict = classification_report(
        y_eval,
        y_pred,
        labels=labels,
        output_dict=True,
        zero_division=0,
    )
    cm = confusion_matrix(y_eval, y_pred, labels=labels)

    metrics: dict[str, float] = {
        "accuracy": float(accuracy_score(y_eval, y_pred)),
        "macro_f1": float(f1_score(y_eval, y_pred, average="macro", zero_division=0)),
        "n_samples": float(len(df)),
        "n_labels": float(len(labels)),
        "cv_n_splits": float(n_splits),
    }
    for label in labels:
        stats = report_dict.get(label, {})
        if isinstance(stats, dict):
            for stat_key in ("precision", "recall", "f1-score", "support"):
                raw = stats.get(stat_key)
                if raw is not None:
                    safe_label = label.replace(" ", "_")
                    metrics[f"{safe_label}_{stat_key.replace('-', '_')}"] = float(raw)

    params = {
        "classifier": "RandomForestClassifier",
        "eval_method": eval_method,
        "train_test_random_state": 42,
        **{f"rf_{k}": v for k, v in RF_PARAMS.items()},
    }

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, ARTIFACT_DIR / "model.joblib")
    joblib.dump(pipeline, ROOT / "model.pkl")
    with open(ARTIFACT_DIR / "label_names.json", "w", encoding="utf-8") as f:
        json.dump(labels, f, indent=2)
    with open(ROOT / "label_names.json", "w", encoding="utf-8") as f:
        json.dump(labels, f, indent=2)
    meta = {
        "n_samples": len(df),
        "labels": labels,
        "feature_names": FEATURE_NAMES,
        "eval_method": eval_method,
        "cv_n_splits": n_splits,
        "metrics": {k: metrics[k] for k in ("accuracy", "macro_f1", "n_samples", "n_labels")},
        "hyperparameters": RF_PARAMS,
    }
    with open(ARTIFACT_DIR / "training_meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    with open(ARTIFACT_DIR / "classification_report.json", "w", encoding="utf-8") as f:
        json.dump(report_dict, f, indent=2)
    with open(ARTIFACT_DIR / "confusion_matrix.json", "w", encoding="utf-8") as f:
        json.dump({"labels": labels, "matrix": cm.tolist()}, f, indent=2)
    df.to_csv(ARTIFACT_DIR / "training_rows.csv", index=False)
    print(f"Wrote {ROOT / 'model.pkl'} and artifacts under {ARTIFACT_DIR}")
    return pipeline, {"params": params, "metrics": metrics, "labels": labels}


def _filter_kernel_argv(argv: list[str]) -> list[str]:
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
    parser = argparse.ArgumentParser(description="Train LiDAR vessel classifier.")
    parser.add_argument(
        "--s3-prefix",
        default=os.environ.get("LIDAR_S3_PREFIX", DEFAULT_S3_PREFIX),
    )
    parser.add_argument("--local-dir", default=os.environ.get("LIDAR_LOCAL_DIR"))
    parser.add_argument(
        "--max-files",
        type=int,
        default=int(os.environ["LIDAR_MAX_FILES"]) if os.environ.get("LIDAR_MAX_FILES") else None,
    )
    parser.add_argument("--no-las-magic-check", action="store_true")
    args = parser.parse_args(_filter_kernel_argv(argv if argv is not None else sys.argv[1:]))

    if args.local_dir:
        df = load_training_table_from_dir(Path(args.local_dir), args.max_files)
    else:
        df = load_training_table(
            args.s3_prefix,
            args.max_files,
            require_las_magic=not args.no_las_magic_check,
        )
    train_and_save(df)[0]


if __name__ == "__main__":
    main()
