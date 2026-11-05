"""Rule-based AIS outlier detection (15-minute ingest windows)."""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from pyproj import Geod

_GEOD = Geod(ellps="WGS84")

RENDEZVOUS_KM = 5.0
MOORED_STATUSES = frozenset({"Moored", "Anchored", "Aground"})
UNDERWAY_STATUSES = frozenset({"Underway using engine", "Underway", "Underway sailing"})
MAX_PLAUSIBLE_SPEED_KN = 22.0
ERRATIC_STATUS_CHANGES = 2
COURSE_JUMP_DEG = 45.0
SPEED_JUMP_KN = 8.0

RECOMMENDATIONS = {
    "Rendezvous": (
        "Verify the proximity of the vessels and maintain safe separation per COLREGS"
    ),
    "Misreported Status": (
        "Cross-check AIS status against speed/position; request corrected AIS or visual confirm"
    ),
    "Erratic Status Changes": (
        "Review track history for equipment fault, spoofing, or manual AIS entry errors"
    ),
    "Inconsistent Maneuvering": (
        "Validate navigation intent; compare with radar/VTMS and prior movement pattern"
    ),
}


def _distance_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    _, _, dist_m = _GEOD.inv(lon1, lat1, lon2, lat2)
    return abs(dist_m) / 1000.0


def _course_delta(a: float, b: float) -> float:
    d = abs(a - b) % 360.0
    return min(d, 360.0 - d)


def _misreported(row: Dict[str, Any]) -> str | None:
    status = row["Status"]
    speed = float(row["Speed"])
    if status in MOORED_STATUSES and speed > 3.0:
        return (
            f"Status '{status}' but SOG {speed:.1f} kn indicates movement"
        )
    if status in UNDERWAY_STATUSES and speed < 0.3 and "engine" in status.lower():
        return f"Status '{status}' but SOG {speed:.1f} kn is near zero"
    if speed > MAX_PLAUSIBLE_SPEED_KN:
        return f"SOG {speed:.1f} kn exceeds plausible merchant AIS for this feed"
    return None


def detect_rule_outliers(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return outlier rows with Reason assigned (may duplicate MMSI for multiple types)."""
    if not records:
        return []

    by_mmsi: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in records:
        by_mmsi[int(row["MMSI"])].append(row)

    outliers: List[Dict[str, Any]] = []
    seen_keys: set[Tuple[int, str, str]] = set()

    def emit(row: Dict[str, Any], otype: str, reason: str, base_severity: int) -> None:
        key = (int(row["MMSI"]), otype, reason[:80])
        if key in seen_keys:
            return
        seen_keys.add(key)
        out = dict(row)
        out["Outlier_Type"] = otype
        out["Reason"] = reason
        out["Recommendation"] = RECOMMENDATIONS[otype]
        out["Severity_Level"] = base_severity
        outliers.append(out)

    # Rendezvous — same timestamp bucket, pairwise distance
    by_ts: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in records:
        by_ts[str(row["Event_Timestamp"])].append(row)
    for ts, group in by_ts.items():
        for i, a in enumerate(group):
            for b in group[i + 1 :]:
                d_km = _distance_km(
                    float(a["Longitude"]),
                    float(a["Latitude"]),
                    float(b["Longitude"]),
                    float(b["Latitude"]),
                )
                if d_km >= RENDEZVOUS_KM:
                    continue
                reason_a = (
                    f"Two vessels (MMSI: {a['MMSI']} and {b['MMSI']}) are "
                    f"less than {RENDEZVOUS_KM:.0f} km apart at {ts}"
                )
                reason_b = (
                    f"Two vessels (MMSI: {b['MMSI']} and {a['MMSI']}) are "
                    f"less than {RENDEZVOUS_KM:.0f} km apart at {ts}"
                )
                emit(a, "Rendezvous", reason_a, 70)
                emit(b, "Rendezvous", reason_b, 70)

    # Per-MMSI rules — use latest row per MMSI for output fields
    for mmsi, rows in by_mmsi.items():
        rows_sorted = sorted(rows, key=lambda r: str(r["Event_Timestamp"]))
        latest = rows_sorted[-1]
        statuses = [str(r["Status"]) for r in rows_sorted]
        unique_statuses = list(dict.fromkeys(statuses))
        status_changes = max(0, len(unique_statuses) - 1)

        mis = _misreported(latest)
        if mis:
            emit(latest, "Misreported Status", mis, 65)

        for r in rows_sorted:
            m = _misreported(r)
            if m and r is not latest:
                emit(r, "Misreported Status", m, 60)

        if status_changes >= ERRATIC_STATUS_CHANGES:
            emit(
                latest,
                "Erratic Status Changes",
                (
                    f"MMSI {mmsi} changed AIS status {status_changes} times "
                    f"in window ({' -> '.join(unique_statuses[:5])})"
                ),
                55,
            )

        speeds = [float(r["Speed"]) for r in rows_sorted]
        courses = [float(r["Course"]) for r in rows_sorted]
        if len(rows_sorted) >= 2:
            max_course_jump = max(
                _course_delta(courses[i], courses[i + 1])
                for i in range(len(courses) - 1)
            )
            max_speed_jump = max(
                abs(speeds[i + 1] - speeds[i]) for i in range(len(speeds) - 1)
            )
            avg_speed = sum(speeds) / len(speeds)
            if max_course_jump >= COURSE_JUMP_DEG and avg_speed > 4.0:
                emit(
                    latest,
                    "Inconsistent Maneuvering",
                    (
                        f"MMSI {mmsi} course change up to {max_course_jump:.0f}° "
                        f"with average SOG {avg_speed:.1f} kn"
                    ),
                    58,
                )
            elif max_speed_jump >= SPEED_JUMP_KN and avg_speed > 3.0:
                emit(
                    latest,
                    "Inconsistent Maneuvering",
                    (
                        f"MMSI {mmsi} speed swing up to {max_speed_jump:.1f} kn "
                        f"within the ingest window"
                    ),
                    52,
                )

    return outliers
