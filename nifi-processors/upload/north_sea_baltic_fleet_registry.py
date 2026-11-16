"""Deterministic North Sea ↔ Baltic trade fleet (up to 2000 ships, MMSI 311040001+)."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

NSB_TRADE_STATE_FILE = "/tmp/nsb_trade_state.json"
SIMULATION_ROUTE_NAME = "north_sea_baltic_trade"

MMSI_BASE = 311_040_001
MAX_FLEET_SLOTS = 2000

VESSEL_TYPES: Tuple[str, ...] = (
    "Container",
    "Bulk Carrier",
    "Tanker",
    "Chemical Tanker",
    "LNG Carrier",
    "Ro-Ro",
    "General Cargo",
)

FLAGS: Tuple[str, ...] = (
    "Liberia",
    "Panama",
    "Marshall Islands",
    "Bahamas",
    "Malta",
    "Cyprus",
    "Denmark",
    "Norway",
    "Germany",
    "Netherlands",
)

NAME_PREFIXES = (
    "Nord",
    "Baltic",
    "Kattegat",
    "Skagerrak",
    "Elbe",
    "Weser",
    "Maersk",
    "Nordic",
    "Scandi",
    "Hanse",
)

NAME_SUFFIXES = (
    "Trader",
    "Carrier",
    "Navigator",
    "Venture",
    "Spirit",
    "Express",
    "Star",
    "Breeze",
    "Hauler",
    "Link",
)

# North Sea load/unload hubs (WGS84).
NORTH_SEA_HUBS: List[Dict[str, float | str]] = [
    {"name": "Hamburg", "Latitude": 53.551, "Longitude": 9.993},
    {"name": "Bremerhaven", "Latitude": 53.539, "Longitude": 8.580},
    {"name": "Wilhelmshaven", "Latitude": 53.513, "Longitude": 8.145},
    {"name": "Emden", "Latitude": 53.367, "Longitude": 7.206},
    {"name": "Cuxhaven", "Latitude": 53.867, "Longitude": 8.697},
    {"name": "Rotterdam", "Latitude": 51.949, "Longitude": 4.145},
    {"name": "Antwerp", "Latitude": 51.263, "Longitude": 4.397},
    {"name": "IJmuiden", "Latitude": 52.463, "Longitude": 4.556},
]

# Baltic freight / tanker / container ports.
BALTIC_TRADE_PORTS: List[Dict[str, float | str]] = [
    {"name": "Rostock", "Latitude": 54.0887, "Longitude": 12.1405},
    {"name": "Travemünde", "Latitude": 53.9624, "Longitude": 10.8672},
    {"name": "Kiel", "Latitude": 54.3233, "Longitude": 10.1228},
    {"name": "Gdynia", "Latitude": 54.5189, "Longitude": 18.5305},
    {"name": "Gdańsk", "Latitude": 54.3520, "Longitude": 18.6466},
    {"name": "Helsinki", "Latitude": 60.1695, "Longitude": 24.9354},
    {"name": "Tallinn", "Latitude": 59.4370, "Longitude": 24.7536},
    {"name": "Riga", "Latitude": 56.9496, "Longitude": 24.1052},
    {"name": "Klaipėda", "Latitude": 55.7033, "Longitude": 21.1443},
    {"name": "Sassnitz", "Latitude": 54.5183, "Longitude": 13.6414},
    {"name": "Karlskrona", "Latitude": 56.1612, "Longitude": 15.5869},
    {"name": "Primorsk", "Latitude": 60.338, "Longitude": 28.480},
]

HUB_BY_NAME: Dict[str, Dict[str, float | str]] = {str(h["name"]): h for h in NORTH_SEA_HUBS}
BALTIC_BY_NAME: Dict[str, Dict[str, float | str]] = {str(p["name"]): p for p in BALTIC_TRADE_PORTS}


def _lcg(seed: int) -> int:
    return (seed * 1_103_515_245 + 12_345) & 0x7FFFFFFF


def fleet_unit_for_index(index: int) -> Dict[str, Any]:
    """Deterministic registry row for slot ``0 .. MAX_FLEET_SLOTS-1``."""
    slot = int(index) % MAX_FLEET_SLOTS
    seed = slot * 17_981 + 42
    vtype = VESSEL_TYPES[_lcg(seed) % len(VESSEL_TYPES)]
    seed = _lcg(seed)
    flag = FLAGS[seed % len(FLAGS)]
    seed = _lcg(seed)
    ns_hub = NORTH_SEA_HUBS[slot % len(NORTH_SEA_HUBS)]
    seed = _lcg(seed)
    baltic = BALTIC_TRADE_PORTS[(slot // len(NORTH_SEA_HUBS)) % len(BALTIC_TRADE_PORTS)]
    prefix = NAME_PREFIXES[_lcg(seed) % len(NAME_PREFIXES)]
    seed = _lcg(seed)
    suffix = NAME_SUFFIXES[seed % len(NAME_SUFFIXES)]
    name = f"{prefix} {suffix} {slot + 1:04d}"
    mmsi = str(MMSI_BASE + slot)
    seed = _lcg(seed)
    length_m = 120.0 + (seed % 220)
    seed = _lcg(seed)
    beam_m = 18.0 + (seed % 28)
    gt = length_m * beam_m * (4.5 + (seed % 20) / 10.0)
    year_built = 1995 + (_lcg(seed + 7) % 29)
    imo = f"IMO{9800000 + slot}"
    call = f"NSB{slot:04d}"
    speed_base = {
        "Container": 18.0,
        "Bulk Carrier": 13.0,
        "Tanker": 12.5,
        "Chemical Tanker": 13.5,
        "LNG Carrier": 15.0,
        "Ro-Ro": 16.0,
        "General Cargo": 11.0,
    }.get(vtype, 12.0)
    dwell_ns = {
        "Container": 16,
        "Bulk Carrier": 28,
        "Tanker": 20,
        "Chemical Tanker": 22,
        "LNG Carrier": 36,
        "Ro-Ro": 12,
        "General Cargo": 24,
    }.get(vtype, 18)
    dwell_baltic = dwell_ns + (_lcg(seed + 3) % 12)
    return {
        "name": name,
        "mmsi": mmsi,
        "vessel_type": vtype,
        "imo_number": imo,
        "call_sign": call,
        "flag": flag,
        "length_m": round(length_m, 1),
        "beam_m": round(beam_m, 1),
        "gross_tonnage": round(gt, 1),
        "year_built": year_built,
        "home_port": str(ns_hub["name"]),
        "north_sea_hub": str(ns_hub["name"]),
        "baltic_port": str(baltic["name"]),
        "speed_base_kn": speed_base,
        "dwell_ns_hours": dwell_ns,
        "dwell_baltic_hours": dwell_baltic,
    }


def mmsi_for_slot(slot: int) -> str:
    return str(MMSI_BASE + int(slot) % MAX_FLEET_SLOTS)