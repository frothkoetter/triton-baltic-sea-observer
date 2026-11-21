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


# Keys = NiFi property ``Cable Asset`` (sorted in DasCableAcousticSimulator).
# WKT source: cdw-analyse/trino-spatial-ais/sql/03_ostsee_infrastruktur.sql → defense.ostsee_infrastruktur

_CABLE_ROWS: List[Dict[str, Any]] = [
    {
        "key": "BALTICCONNECTOR",
        "id": "EMODNET_BALTICCONNECTOR",
        "name": "Balticconnector",
        "typ": "Gaspipeline",
        "medium": "Gas",
        "monitoring_use_case": "anchor_drag_and_cui_sabotage",
        "wkt": (
            "LINESTRING (24.08999047 59.95196291, 24.10611111 59.90552432, "
            "24.03913381 59.86896759, 24.03472785 59.77162482, 24.12802207 59.50136118, "
            "24.13429474 59.40054982)"
        ),
    },
    {
        "key": "NORD_STREAM_1",
        "id": "EMODNET_NS1",
        "name": "NORD STREAM 1",
        "typ": "Gaspipeline",
        "medium": "Gas",
        "monitoring_use_case": "anchor_drag",
        "wkt": (
            "LINESTRING (23.99979769 59.65753773, 22.52826538 59.45687113, 21.09415781 59.20373063, "
            "21.05036577 59.14071078, 20.79176628 59.00212766, 20.52227967 58.8899604, "
            "20.35897759 58.75928755, 20.33513795 58.70332492, 20.28324883 58.66143174, "
            "20.22280991 58.57277674, 20.16576533 58.45778949, 20.09268027 58.41262821, "
            "19.77499176 57.92896757, 19.65590183 57.7961544, 19.48553558 57.52118732, "
            "19.42811943 57.4765614, 18.82467552 56.55030537, 18.57729168 56.34578188, "
            "18.10737188 56.24843544, 16.4697971 55.66691842, 15.70063987 55.53579754, "
            "15.42628948 55.38311764, 15.36550871 55.21530088, 15.21641767 54.93645624, "
            "14.79831532 54.82518868, 14.49431612 54.60762594, 14.21395931 54.54142768, "
            "14.08479726 54.5332317, 13.71688492 54.24308183, 13.70184897 54.21301225, "
            "13.63614605 54.19030182, 13.63106352 54.1715519, 13.65426161 54.15419366)"
        ),
    },
    {
        "key": "NORD_STREAM_2",
        "id": "EMODNET_NS2",
        "name": "NORD STREAM 2",
        "typ": "Gaspipeline",
        "medium": "Gas",
        "monitoring_use_case": "anchor_drag",
        "wkt": (
            "LINESTRING (28.14227953 60.52449418, 28.08942727 60.38930494, 27.88258696 60.2229997, "
            "27.51118065 60.1059404, 26.75406533 60.06940664, 25.89021281 59.84955768, "
            "24.95805292 59.83262487, 23.91754305 59.59039612, 21.2288291 59.17345674, "
            "20.42120071 58.73807712, 20.22016814 58.42254669, 18.9172008 56.58500187, "
            "18.6313395 56.31518911, 18.09941614 56.18138567, 16.39890865 55.60140081, "
            "15.77565632 55.50692414, 15.51506379 55.38733518, 15.28232415 54.90765986, "
            "14.85696937 54.79483342, 14.55184069 54.5663022, 14.11857926 54.49406361, "
            "13.72166074 54.2354678, 13.71325424 54.20953787, 13.63155895 54.18466936, "
            "13.66381217 54.15715816)"
        ),
    },
    {
        "key": "PCABLE_BSH_1",
        "id": "EMODNET_PCABLE_BSH_1",
        "name": "Offshore power cable (BSH/CONTIS) - segment 1",
        "typ": "Seekabel",
        "medium": "Power",
        "monitoring_use_case": "cui_tapping",
        "wkt": (
            "LINESTRING (10.90364984 53.95650018, 10.9191502 53.97384028, 11.27548958 54.12651006, "
            "11.71548958 54.29168972, 12.08401006 54.29203018, 12.22384028 54.3916996, "
            "12.36051024 54.52486974, 12.6071802 54.67171038, 12.6408584 54.75531389)"
        ),
    },
    {
        "key": "PCABLE_BSH_2",
        "id": "EMODNET_PCABLE_BSH_2",
        "name": "Offshore power cable (BSH/CONTIS) - segment 2",
        "typ": "Seekabel",
        "medium": "Power",
        "monitoring_use_case": "cui_tapping",
        "wkt": (
            "LINESTRING (13.65334547 54.15833425, 13.6471388 54.16392291, 13.65039886 54.18535332, "
            "13.72114047 54.209195, 13.72543203 54.2374458, 13.8521765 54.32908812, "
            "13.83924288 54.34864351, 13.86474657 54.37118565, 13.8573013 54.38355352, "
            "13.84767269 54.50691848, 13.82201351 54.61820671, 13.87355762 54.72281662, "
            "13.93517153 54.75760984, 13.69526362 54.82014481, 13.68019275 54.81793704)"
        ),
    },
    {
        "key": "PCABLE_BSH_3",
        "id": "EMODNET_PCABLE_BSH_3",
        "name": "Offshore power cable (BSH/CONTIS) - segment 3",
        "typ": "Seekabel",
        "medium": "Power",
        "monitoring_use_case": "cui_tapping",
        "wkt": (
            "LINESTRING (13.65904106 54.15418313, 13.65157662 54.16214353, 13.65447918 54.18389494, "
            "13.72556072 54.20741299, 13.72869218 54.2361995, 13.85757551 54.32839299, "
            "13.8447444 54.34886923, 13.87210358 54.37126522, 13.8593145 54.40321336, "
            "13.85233544 54.50737797, 13.83133606 54.61758676, 13.88167135 54.71999024, "
            "13.94404829 54.75402269, 13.94487569 54.76889469, 14.03101089 54.7802854, "
            "14.03452444 54.79647781, 14.09079304 54.8063018, 14.08239963 54.82000882, "
            "14.08573124 54.83089995, 14.0737507 54.83464922)"
        ),
    },
    {
        "key": "PCABLE_BSH_4",
        "id": "EMODNET_PCABLE_BSH_4",
        "name": "Offshore power cable (BSH/CONTIS) - segment 4",
        "typ": "Seekabel",
        "medium": "Power",
        "monitoring_use_case": "cui_tapping",
        "wkt": (
            "LINESTRING (13.65892037 54.15411266, 13.65006995 54.16298981, 13.65302761 54.18440004, "
            "13.72451092 54.20822215, 13.72811924 54.23653393, 13.85575266 54.32879564, "
            "13.84238285 54.34807076, 13.87038855 54.37134365, 13.86050454 54.3877265, "
            "13.85078977 54.50746368, 13.82822298 54.61741701, 13.87880928 54.72075164, "
            "13.94050529 54.75448205, 13.94109647 54.77022477, 14.0299514 54.78159185, "
            "14.01889748 54.82548224, 14.07389896 54.83471551)"
        ),
    },
]

CABLE_ASSETS: Dict[str, Dict[str, Any]] = {
    row["key"]: {k: v for k, v in row.items() if k != "key"} | {"status": "In use"}
    for row in _CABLE_ROWS
}


def cable_asset_keys() -> List[str]:
    """Sorted keys for NiFi ``allowable_values`` on Cable Asset."""
    return sorted(CABLE_ASSETS.keys())


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