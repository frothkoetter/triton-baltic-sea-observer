"""Feature vectors for optional IsolationForest severity calibration."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List

import numpy as np

from src.rules import _course_delta, _distance_km


def batch_feature_matrix(records: List[Dict[str, Any]]) -> np.ndarray:
    """One row per record: speed, course, min_dist_km, status_code, mmsi_change_rate."""
    if not records:
        return np.zeros((0, 5), dtype=float)

    by_mmsi: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in records:
        by_mmsi[int(row["MMSI"])].append(row)

    status_map = {
        "Moored": 0,
        "Anchored": 1,
        "Underway using engine": 2,
        "Underway": 3,
        "Underway sailing": 4,
    }

    rows: List[List[float]] = []
    for row in records:
        mmsi = int(row["MMSI"])
        speed = float(row["Speed"])
        course = float(row["Course"])
        status_code = float(status_map.get(str(row["Status"]), 5))

        min_dist = 999.0
        for other in records:
            if int(other["MMSI"]) == mmsi:
                continue
            d = _distance_km(
                float(row["Longitude"]),
                float(row["Latitude"]),
                float(other["Longitude"]),
                float(other["Latitude"]),
            )
            min_dist = min(min_dist, d)

        hist = by_mmsi[mmsi]
        change_rate = 0.0
        if len(hist) >= 2:
            statuses = [str(h["Status"]) for h in hist]
            change_rate = float(len(set(statuses)) - 1)
            courses = [float(h["Course"]) for h in hist]
            change_rate += max(
                _course_delta(courses[i], courses[i + 1])
                for i in range(len(courses) - 1)
            ) / 90.0

        rows.append([speed, course, min_dist, status_code, change_rate])

    return np.asarray(rows, dtype=float)
