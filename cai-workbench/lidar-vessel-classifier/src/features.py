from __future__ import annotations

import io
from typing import Tuple

import numpy as np

try:
    from src.labels import FEATURE_NAMES
except ImportError:
    from labels import FEATURE_NAMES


def _shape_descriptors(cov: np.ndarray) -> Tuple[float, float, float]:
    """Eigenvalue-based linearity / planarity / sphericity (0–1 scale)."""
    evals = np.linalg.eigvalsh(cov)
    evals = np.sort(np.maximum(evals, 1e-12))[::-1]
    l1, l2, l3 = evals[0], evals[1], evals[2]
    linearity = (l1 - l2) / l1
    planarity = (l2 - l3) / l1
    sphericity = l3 / l1
    return float(linearity), float(planarity), float(sphericity)


def las_points_to_feature_vector(
    x: np.ndarray,
    y: np.ndarray,
    z: np.ndarray,
    intensity: np.ndarray | None,
    classification: np.ndarray | None,
) -> np.ndarray:
    n = len(x)
    if n == 0:
        return np.zeros(len(FEATURE_NAMES), dtype=np.float64)

    pts = np.column_stack([x, y, z]).astype(np.float64)
    centroid = pts.mean(axis=0)
    centered = pts - centroid
    extent = pts.max(axis=0) - pts.min(axis=0)

    if intensity is None or len(intensity) == 0:
        mean_i, std_i = 0.0, 0.0
    else:
        mean_i = float(np.mean(intensity))
        std_i = float(np.std(intensity))

    if classification is None or len(classification) == 0:
        dominant_ratio = 0.0
    else:
        values, counts = np.unique(classification, return_counts=True)
        dominant_ratio = float(counts.max() / counts.sum())

    if n >= 3:
        cov = np.cov(centered.T)
        linearity, planarity, sphericity = _shape_descriptors(cov)
    else:
        linearity, planarity, sphericity = 0.0, 0.0, 0.0

    return np.array(
        [
            float(n),
            float(extent[0]),
            float(extent[1]),
            float(extent[2]),
            float(centroid[2]),
            float(np.std(z)),
            mean_i,
            std_i,
            linearity,
            planarity,
            sphericity,
            dominant_ratio,
        ],
        dtype=np.float64,
    )


def extract_features_from_las_bytes(payload: bytes) -> np.ndarray:
    import laspy

    with laspy.open(io.BytesIO(payload)) as reader:
        las = reader.read()
    x = np.asarray(las.x)
    y = np.asarray(las.y)
    z = np.asarray(las.z)
    intensity = np.asarray(las.intensity) if hasattr(las, "intensity") else None
    classification = np.asarray(las.classification) if hasattr(las, "classification") else None
    if classification is not None and len(classification):
        try:
            from src.labels import CODE_TO_VESSEL_TYPE
        except ImportError:
            from labels import CODE_TO_VESSEL_TYPE
        vessel_mask = np.isin(classification, list(CODE_TO_VESSEL_TYPE.keys()))
        if vessel_mask.any():
            x, y, z = x[vessel_mask], y[vessel_mask], z[vessel_mask]
            if intensity is not None:
                intensity = intensity[vessel_mask]
            classification = classification[vessel_mask]
    return las_points_to_feature_vector(x, y, z, intensity, classification)


def ground_truth_label_from_las_bytes(payload: bytes) -> str | None:
    """Majority LAS classification code → vessel type name."""
    import laspy

    try:
        from src.labels import CODE_TO_VESSEL_TYPE
    except ImportError:
        from labels import CODE_TO_VESSEL_TYPE

    with laspy.open(io.BytesIO(payload)) as reader:
        las = reader.read()
    if not hasattr(las, "classification"):
        return None
    cls = np.asarray(las.classification)
    if cls.size == 0:
        return None
    vessel_cls = cls[np.isin(cls, list(CODE_TO_VESSEL_TYPE.keys()))]
    if vessel_cls.size == 0:
        return None
    values, counts = np.unique(vessel_cls, return_counts=True)
    code = int(values[counts.argmax()])
    return CODE_TO_VESSEL_TYPE.get(code)
