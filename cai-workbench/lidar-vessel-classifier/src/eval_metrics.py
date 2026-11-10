"""Replay evaluation on saved feature rows (artifacts/training_rows.csv)."""

from __future__ import annotations

import json
from pathlib import Path

import joblib
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
)

from src.labels import FEATURE_NAMES


def evaluate_saved_model(
    model_path: Path,
    rows_path: Path,
    out_dir: Path | None = None,
) -> dict:
    pipeline = joblib.load(model_path)
    df = pd.read_csv(rows_path)
    if df.empty:
        raise RuntimeError(f"No rows in {rows_path}")
    labels = sorted(df["label"].unique().tolist())
    X = df[FEATURE_NAMES].values
    y = df["label"].values
    y_pred = pipeline.predict(X)

    report_dict = classification_report(
        y, y_pred, labels=labels, output_dict=True, zero_division=0
    )
    cm = confusion_matrix(y, y_pred, labels=labels)
    metrics: dict[str, float] = {
        "accuracy": float(accuracy_score(y, y_pred)),
        "macro_f1": float(f1_score(y, y_pred, average="macro", zero_division=0)),
        "n_samples": float(len(df)),
        "n_labels": float(len(labels)),
    }
    for label in labels:
        stats = report_dict.get(label, {})
        if isinstance(stats, dict):
            for stat_key in ("precision", "recall", "f1-score", "support"):
                raw = stats.get(stat_key)
                if raw is not None:
                    safe_label = label.replace(" ", "_")
                    metrics[f"{safe_label}_{stat_key.replace('-', '_')}"] = float(raw)

    out_dir = out_dir or rows_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    eval_report = out_dir / "eval_classification_report.json"
    eval_cm = out_dir / "eval_confusion_matrix.json"
    eval_report.write_text(json.dumps(report_dict, indent=2), encoding="utf-8")
    eval_cm.write_text(
        json.dumps({"labels": labels, "matrix": cm.tolist()}, indent=2),
        encoding="utf-8",
    )
    return {
        "metrics": metrics,
        "artifact_paths": [eval_report, eval_cm],
        "params": {"eval_rows": str(rows_path), "model_path": str(model_path)},
    }
