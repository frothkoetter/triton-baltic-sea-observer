from __future__ import annotations

from typing import Dict, List

import numpy as np

from .labels import FEATURE_NAMES


def features_from_summary(summary: Dict[str, float]) -> np.ndarray:
    return np.array([float(summary.get(k, 0.0)) for k in FEATURE_NAMES], dtype=np.float64)


def features_from_hdf5_bytes(payload: bytes) -> List[Dict[str, object]]:
    """Feature rows from NiFi DAS HDF5 FlowFile (same schema as upload/das_acoustic_engine)."""
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]  # repo root
    upload = root / "nifi-processors" / "upload"
    if upload.as_posix() not in sys.path:
        sys.path.insert(0, upload.as_posix())
    from das_acoustic_engine import read_hdf5_features_from_bytes

    rows = read_hdf5_features_from_bytes(payload)
    return rows