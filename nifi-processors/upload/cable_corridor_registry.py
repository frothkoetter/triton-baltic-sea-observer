"""Submarine cable corridors (EMODnet WKT from cdw-analyse/trino-spatial-ais/sql/03_ostsee_infrastruktur.sql)."""

from __future__ import annotations

import math
import re
from typing import Any, Dict, List, Tuple

Coord = Tuple[float, float]  # lon, lat


def _parse_linestring(wkt: str) -> List[Coord]:
    m = re.search(r"LINESTRING\s*\((.*)\)", wkt, re.I | re.S)
    if not m:
        raise ValueError(f"Unsupported WKT: {wkt[:80]}")
    coords: List[Coord] = []
    for pair in m.group(1).split(","):
        parts = pair.strip().split()
        if len(parts) >= 2:
            coords.append((float(parts[0]), float(parts[1])))
    if len(coords) < 2:
        raise ValueError("LINESTRING needs at least 2 vertices")
    return coords


def _haversine_m(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    r = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def chainage_along_line(vertices: List[Coord]) -> List[Dict[str, float]]:
    """Cumulative chainage (m) and WGS84 at each vertex + interpolated points."""
    if not vertices:
        return []
    out: List[Dict[str, float]] = []
    cum = 0.0
    out.append({"chainage_m": cum, "lon": vertices[0][0], "lat": vertices[0][1]})
    for i in range(1, len(vertices)):
        lon1, lat1 = vertices[i - 1]
        lon2, lat2 = vertices[i]
        seg = _haversine_m(lon1, lat1, lon2, lat2)
        cum += seg
        out.append({"chainage_m": cum, "lon": lon2, "lat": lat2})
    return out


def interpolate_chainage(vertices: List[Coord], chainage_m: float) -> Dict[str, float]:
    """Position at distance *chainage_m* along the polyline."""
    pts = chainage_along_line(vertices)
    if chainage_m <= 0:
        p = pts[0]
        return {"chainage_m": 0.0, "lon": p["lon"], "lat": p["lat"]}
    if chainage_m >= pts[-1]["chainage_m"]:
        p = pts[-1]
        return {"chainage_m": p["chainage_m"], "lon": p["lon"], "lat": p["lat"]}
    for i in range(1, len(pts)):
        if pts[i]["chainage_m"] >= chainage_m:
            a, b = pts[i - 1], pts[i]
            span = b["chainage_m"] - a["chainage_m"]
            f = (chainage_m - a["chainage_m"]) / span if span > 0 else 0.0
            return {
                "chainage_m": chainage_m,
                "lon": a["lon"] + f * (b["lon"] - a["lon"]),
                "lat": a["lat"] + f * (b["lat"] - a["lat"]),
            }
    p = pts[-1]
    return {"chainage_m": p["chainage_m"], "lon": p["lon"], "lat": p["lat"]}


CABLE_ASSETS: Dict[str, Dict[str, Any]] = {
    "BALTICCONNECTOR": {
        "id": "EMODNET_BALTICCONNECTOR",
        "name": "Balticconnector",
        "typ": "Gaspipeline",
        "medium": "Gas",
        "status": "In use",
        "monitoring_use_case": "anchor_drag_and_cui_sabotage",
        "wkt": (
            "LINESTRING (24.08999047 59.95196291, 24.10611111 59.90552432, "
            "24.03913381 59.86896759, 24.03472785 59.77162482, 24.12802207 59.50136118, "
            "24.13429474 59.40054982)"
        ),
    },
    "NORD_STREAM_1": {
        "id": "EMODNET_NS1",
        "name": "NORD STREAM 1",
        "typ": "Gaspipeline",
        "medium": "Gas",
        "status": "In use",
        "monitoring_use_case": "anchor_drag",
        "wkt": (
            "LINESTRING (23.99979769 59.65753773, 22.52826538 59.45687113, "
            "21.09415781 59.20373063, 20.79176628 59.00212766, 18.82467552 56.55030537, "
            "15.21641767 54.93645624, 13.65426161 54.15419366)"
        ),
    },
    "PCABLE_BSH_1": {
        "id": "EMODNET_PCABLE_BSH_1",
        "name": "Offshore power cable (BSH/CONTIS) - segment 1",
        "typ": "Seekabel",
        "medium": "Power",
        "status": "In use",
        "monitoring_use_case": "cui_tapping",
        "wkt": (
            "LINESTRING (10.90364984 53.95650018, 11.27548958 54.12651006, "
            "12.08401006 54.29203018, 12.6408584 54.75531389)"
        ),
    },
}


def cable_vertices(asset_key: str) -> List[Coord]:
    key = asset_key.upper()
    if key not in CABLE_ASSETS:
        raise KeyError(f"Unknown cable asset: {asset_key}")
    return _parse_linestring(CABLE_ASSETS[key]["wkt"])


def cable_length_m(asset_key: str) -> float:
    verts = cable_vertices(asset_key)
    pts = chainage_along_line(verts)
    return pts[-1]["chainage_m"]


def cable_meta(asset_key: str) -> Dict[str, Any]:
    key = asset_key.upper()
    meta = dict(CABLE_ASSETS[key])
    meta["length_m"] = cable_length_m(key)
    meta["vertices"] = len(cable_vertices(key))
    return meta