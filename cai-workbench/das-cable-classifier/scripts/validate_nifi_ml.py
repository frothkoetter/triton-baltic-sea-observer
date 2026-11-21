#!/usr/bin/env python3
"""
End-to-end: NiFi DAS HDF5 batch → feature extraction → trained classifier.

Run from repo root after `python3 train.py` in das-cable-classifier:

    python3 cai-workbench/das-cable-classifier/scripts/validate_nifi_ml.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]  # repo root
NIFI_UPLOAD = REPO / "nifi-processors" / "upload"
for p in (ROOT.as_posix(), NIFI_UPLOAD.as_posix()):
    if p not in sys.path:
        sys.path.insert(0, p)

import joblib
from das_acoustic_engine import generate_das_batch, write_hdf5_batch, read_hdf5_features_from_bytes

from src.features import features_from_summary  # noqa: E402
from src.labels import FEATURE_NAMES  # noqa: E402


def main() -> None:
    model_path = ROOT / "model.pkl"
    if not model_path.is_file():
        model_path = ROOT / "artifacts" / "model.joblib"
    if not model_path.is_file():
        print("Run train.py first.", file=sys.stderr)
        sys.exit(1)

    pipe = joblib.load(model_path)
    clf = pipe.named_steps["clf"]

    segments, meta = generate_das_batch("BALTICCONNECTOR", n_segments=6, seed=99)
    h5 = write_hdf5_batch(segments, meta)
    rows = read_hdf5_features_from_bytes(h5)

    print(f"Cable: {meta['cable_name']} ({meta['cable_id']})")
    print(f"HDF5 size: {len(h5) / 1024:.1f} KiB | segments: {len(rows)}\n")
    print(f"{'segment':<12} {'gt':<22} {'pred':<22} {'conf':>6}  match")
    print("-" * 78)

    matches = 0
    report_rows = []
    for row in rows:
        gt = row["threat_class"]
        summary = {k: row[k] for k in FEATURE_NAMES}
        vec = features_from_summary(summary).reshape(1, -1)
        pred = pipe.predict(vec)[0]
        proba = pipe.predict_proba(vec)[0]
        conf = float(proba.max())
        ok = pred == gt
        matches += int(ok)
        seg = row.get("segment", "?")
        print(f"{seg:<12} {gt:<22} {pred:<22} {conf:6.3f}  {'OK' if ok else 'MISS'}")
        report_rows.append({"segment": seg, "ground_truth": gt, "prediction": pred, "confidence": conf})

    acc = matches / max(1, len(rows))
    print("-" * 78)
    print(f"Segment accuracy vs synthetic GT: {acc:.1%} ({matches}/{len(rows)})")
    out = ROOT / "artifacts" / "nifi_ml_validation.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"meta": meta, "results": report_rows, "accuracy": acc}, indent=2), encoding="utf-8")
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()