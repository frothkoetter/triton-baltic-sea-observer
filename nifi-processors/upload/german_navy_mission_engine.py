"""German Navy mission simulation (home base → Baltic patrol → return → 7d in port)."""

from __future__ import annotations

import json
import math
import os
import random
from typing import Any, Dict, List, Optional

from baltic_ship_routing import (
    SIMULATION_HOURS_PER_TICK,
    WAYPOINT_ARRIVAL_M,
    _GEOD,
    build_sea_route,
    move_towards_on_water,
    route_target,
)
from baltic_water_placement import is_baltic_water, sample_water_near_harbor, snap_to_baltic_water

from marine_fleet_registry import (
    GERMAN_NAVAL_FLEET,
    MARINESHIP_STATE_FILE,
    NAME_BY_MMSI,
    UNIT_BY_MMSI,
    fleet_unit_for_index,
)

SIMULATION_ROUTE_NAME = "german_navy_missions"

GERMAN_NAVAL_BASES: Dict[str, Dict[str, float | str]] = {
    "Kiel": {"name": "Kiel", "Latitude": 54.3233, "Longitude": 10.1228},
    "Wilhelmshaven": {"name": "Wilhelmshaven", "Latitude": 53.513, "Longitude": 8.145},
    "Eckernförde": {"name": "Eckernförde", "Latitude": 54.473, "Longitude": 9.838},
    "Warnemünde": {"name": "Warnemünde", "Latitude": 54.176, "Longitude": 12.089},
}

# Coastal / German Bight patrol boxes (minesweepers, small craft, corvettes).
GERMAN_COASTAL_PATROL: List[Dict[str, float | str]] = [
    {"name": "German Bight", "Latitude": 54.25, "Longitude": 7.6},
    {"name": "Helgoland approaches", "Latitude": 54.45, "Longitude": 7.9},
    {"name": "Fehmarn Belt", "Latitude": 54.52, "Longitude": 11.25},
    {"name": "Kiel Bay patrol", "Latitude": 54.58, "Longitude": 10.45},
    {"name": "Mecklenburg coast", "Latitude": 54.12, "Longitude": 12.05},
    {"name": "Rügen offshore", "Latitude": 54.55, "Longitude": 13.45},
]

# Baltic Sea boxes for frigates, submarines, tenders, intelligence.
BALTIC_DEEP_PATROL: List[Dict[str, float | str]] = [
    {"name": "Bornholm gap", "Latitude": 55.05, "Longitude": 15.05},
    {"name": "Central Baltic", "Latitude": 56.45, "Longitude": 17.2},
    {"name": "Gotland south", "Latitude": 57.05, "Longitude": 18.35},
    {"name": "Gulf of Finland approach", "Latitude": 59.55, "Longitude": 20.1},
    {"name": "Arkona basin", "Latitude": 54.85, "Longitude": 14.15},
]

MISSION_ELIGIBLE_FRACTION = 0.33
PORT_STAY_TICKS = int(7 * 24 / SIMULATION_HOURS_PER_TICK)
ANCHOR_MAX_TICKS = max(1, int(24 / SIMULATION_HOURS_PER_TICK))
ANCHOR_PROBABILITY = 0.18
SPEED_FLUCTUATION = 0.4

BLUEWATER_CLASSES = frozenset(
    {"Fregatten", "U-Boote", "Versorgungsschiffe/Tender", "Flottendienstboote"}
)


def at_waypoint(
    lat: float,
    lon: float,
    target_lat: float,
    target_lon: float,
    threshold_m: float | None = None,
) -> bool:
    limit = threshold_m if threshold_m is not None else WAYPOINT_ARRIVAL_M
    _, _, dist_m = _GEOD.inv(lon, lat, target_lon, target_lat)
    return abs(dist_m) <= limit


def vessel_category(class_name: str) -> str:
    if class_name == "U-Boote":
        return "submarine"
    if class_name == "Fregatten":
        return "frigate"
    if class_name == "Korvetten":
        return "corvette"
    if class_name == "Minenabwehreinheiten":
        return "minesweeper"
    if class_name == "Versorgungsschiffe/Tender":
        return "support"
    if class_name == "Flottendienstboote":
        return "auxiliary"
    return "patrol"


def home_base_name_for_unit(unit: Dict[str, str]) -> str:
    cls = unit.get("class", "")
    name = unit.get("name", "")
    if cls == "U-Boote":
        return "Eckernförde"
    if cls == "Minenabwehreinheiten":
        return "Warnemünde"
    if cls == "Sonstige" and any(
        island in name for island in ("Wangerooge", "Baltrum", "Borkum", "Juist", "Langeoog", "Norderney")
    ):
        return "Wilhelmshaven"
    if cls in ("Versorgungsschiffe/Tender",):
        return "Wilhelmshaven" if "Elbe" in name or "Mosel" in name else "Kiel"
    if cls == "Flottendienstboote":
        return "Kiel"
    if cls == "Korvetten":
        return "Warnemünde" if name.endswith(("Oldenburg", "Erfurt")) else "Kiel"
    return "Kiel"


def home_base_for_unit(unit: Dict[str, str]) -> Dict[str, Any]:
    key = home_base_name_for_unit(unit)
    return dict(GERMAN_NAVAL_BASES.get(key, GERMAN_NAVAL_BASES["Kiel"]))


def _mission_tick_bounds(unit: Dict[str, str]) -> tuple[int, int]:
    cls = unit.get("class", "")
    if cls in BLUEWATER_CLASSES or cls == "Fregatten":
        min_d, max_d = 7, 28
    elif cls == "Korvetten":
        min_d, max_d = 7, 21
    else:
        min_d, max_d = 7, 14
    return (
        int(min_d * 24 / SIMULATION_HOURS_PER_TICK),
        int(max_d * 24 / SIMULATION_HOURS_PER_TICK),
    )


def _patrol_pool(unit: Dict[str, str]) -> List[Dict[str, float | str]]:
    cls = unit.get("class", "")
    if cls in BLUEWATER_CLASSES or cls == "Fregatten":
        return list(BALTIC_DEEP_PATROL) + list(GERMAN_COASTAL_PATROL[:2])
    return list(GERMAN_COASTAL_PATROL)


def _pick_patrol_loop(unit: Dict[str, str], rng: random.Random) -> List[Dict[str, Any]]:
    pool = _patrol_pool(unit)
    rng.shuffle(pool)
    count = 5 if unit.get("class") in BLUEWATER_CLASSES else 3
    return [dict(z) for z in pool[: min(count, len(pool))]]


def _pseudo_destination(zone: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": zone["name"],
        "Latitude": float(zone["Latitude"]),
        "Longitude": float(zone["Longitude"]),
    }


def _place_at_base(ship: Dict[str, Any], base: Dict[str, Any]) -> None:
    mmsi = ship.get("MMSI", "")
    lon, lat = sample_water_near_harbor(
        float(base["Longitude"]),
        float(base["Latitude"]),
        min_km=0.5,
        max_km=4.0,
        seed=hash(mmsi) % 10_000,
    )
    ship["Latitude"] = lat
    ship["Longitude"] = lon
    ship["Speed"] = 0.0
    ship["Status"] = "Anchored"
    ship["Destination"] = dict(base)
    ship["RouteWaypoints"] = []
    ship["RouteIndex"] = 0
    ship["Depth"] = 0.0


def _start_mission(ship: Dict[str, Any], unit: Dict[str, str], rng: random.Random) -> None:
    lo, hi = _mission_tick_bounds(unit)
    ship["Phase"] = "mission_patrol"
    ship["MissionTicksRemaining"] = rng.randint(lo, hi)
    ship["PatrolZones"] = _pick_patrol_loop(unit, rng)
    ship["PatrolIndex"] = 0
    ship["AnchorTicksRemaining"] = 0
    ship["PortStayTicksRemaining"] = 0
    zone = ship["PatrolZones"][0]
    ship["Destination"] = _pseudo_destination(zone)
    ship["RouteWaypoints"] = build_sea_route(
        float(ship["Longitude"]),
        float(ship["Latitude"]),
        float(zone["Longitude"]),
        float(zone["Latitude"]),
    )
    ship["RouteIndex"] = 0
    ship["Speed"] = rng.uniform(8.0, 14.0 if unit.get("class") != "U-Boote" else 10.0)
    ship["Status"] = "Underway using engine"
    ship["Depth"] = random.uniform(8.0, 25.0) if unit.get("class") == "U-Boote" else random.uniform(5.0, 15.0)


def _begin_return_home(ship: Dict[str, Any]) -> None:
    base = ship.get("HomeBase") or home_base_for_unit(ship.get("_unit", {}))
    ship["Phase"] = "mission_return"
    ship["Destination"] = dict(base)
    ship["RouteWaypoints"] = build_sea_route(
        float(ship["Longitude"]),
        float(ship["Latitude"]),
        float(base["Longitude"]),
        float(base["Latitude"]),
    )
    ship["RouteIndex"] = 0
    ship["Speed"] = max(float(ship.get("Speed") or 0), 8.0)
    ship["Status"] = "Underway using engine"


def _advance_patrol_leg(ship: Dict[str, Any], lat: float, lon: float, rng: random.Random) -> None:
    wps: List[Dict[str, float]] = ship.get("RouteWaypoints") or []
    idx = int(ship.get("RouteIndex", 0))
    if idx >= len(wps):
        zones: List[Dict[str, Any]] = ship.get("PatrolZones") or []
        pidx = int(ship.get("PatrolIndex", 0))
        if pidx + 1 < len(zones):
            ship["PatrolIndex"] = pidx + 1
            zone = zones[pidx + 1]
            ship["Destination"] = _pseudo_destination(zone)
            ship["RouteWaypoints"] = build_sea_route(
                lon, lat, float(zone["Longitude"]), float(zone["Latitude"])
            )
            ship["RouteIndex"] = 0
        else:
            _begin_return_home(ship)
        return

    tgt_lat, tgt_lon = float(wps[idx]["lat"]), float(wps[idx]["lon"])
    if not at_waypoint(lat, lon, tgt_lat, tgt_lon):
        return

    if rng.random() < ANCHOR_PROBABILITY:
        ship["AnchorTicksRemaining"] = rng.randint(1, ANCHOR_MAX_TICKS)
        ship["Phase"] = "anchored"
        ship["Speed"] = 0.0
        ship["Status"] = "Anchored"
        return

    ship["RouteIndex"] = idx + 1
    if ship["RouteIndex"] >= len(wps):
        zones = ship.get("PatrolZones") or []
        pidx = int(ship.get("PatrolIndex", 0))
        if pidx + 1 < len(zones):
            ship["PatrolIndex"] = pidx + 1
            zone = zones[pidx + 1]
            ship["Destination"] = _pseudo_destination(zone)
            ship["RouteWaypoints"] = build_sea_route(
                lon, lat, float(zone["Longitude"]), float(zone["Latitude"])
            )
            ship["RouteIndex"] = 0
        else:
            _begin_return_home(ship)


def _complete_mission_at_base(ship: Dict[str, Any], lat: float, lon: float, base: Dict[str, Any]) -> None:
    if at_waypoint(lat, lon, float(base["Latitude"]), float(base["Longitude"]), threshold_m=8_000.0):
        ship["Phase"] = "in_port"
        ship["PortStayTicksRemaining"] = PORT_STAY_TICKS
        ship["MissionTicksRemaining"] = 0
        ship["Speed"] = 0.0
        ship["Status"] = "Moored"
        ship["Depth"] = 0.0
        _place_at_base(ship, base)


def enrich_unit(raw: Dict[str, str]) -> Dict[str, str]:
    u = dict(raw)
    u["vessel_category"] = vessel_category(u.get("class", ""))
    u["home_base"] = home_base_name_for_unit(u)
    return u


def initialize_german_navy_ships(
    num_ships: int,
    *,
    rng: Optional[random.Random] = None,
    operational_status_options: List[str],
    system_status_options: List[str],
) -> List[Dict[str, Any]]:
    rng = rng or random.Random()
    n = min(max(1, num_ships), len(GERMAN_NAVAL_FLEET))
    units = [enrich_unit(fleet_unit_for_index(i)) for i in range(n)]
    target_mission = max(1, round(len(units) * MISSION_ELIGIBLE_FRACTION))
    shuffled = list(units)
    rng.shuffle(shuffled)
    mission_mmsi = {u["mmsi"] for u in shuffled[:target_mission]}

    ships: List[Dict[str, Any]] = []
    for unit in units:
        base = home_base_for_unit(unit)
        ship: Dict[str, Any] = {
            "Name": unit["name"],
            "MMSI": unit["mmsi"],
            "Class": unit["class"],
            "HomeBaseName": unit["home_base"],
            "HomeBase": dict(base),
            "_unit": unit,
            "Latitude": base["Latitude"],
            "Longitude": base["Longitude"],
            "Speed": 0.0,
            "Course": rng.uniform(0, 360),
            "Status": "Anchored",
            "Destination": dict(base),
            "OriginPort": dict(base),
            "RouteWaypoints": [],
            "RouteIndex": 0,
            "Phase": "home_port",
            "MissionTicksRemaining": 0,
            "PortStayTicksRemaining": 0,
            "AnchorTicksRemaining": 0,
            "PatrolZones": [],
            "PatrolIndex": 0,
            "Depth": 0.0,
            "Operational_Status": rng.choice(operational_status_options),
            "System_Status": rng.choice(system_status_options),
        }
        _place_at_base(ship, base)
        if unit["mmsi"] in mission_mmsi:
            _start_mission(ship, unit, rng)
        ships.append(ship)
    return ships


def update_german_navy_movements(
    ships: List[Dict[str, Any]],
    *,
    rng: Optional[random.Random] = None,
) -> List[Dict[str, Any]]:
    rng = rng or random.Random()
    for ship in ships:
        unit = ship.get("_unit") or UNIT_BY_MMSI.get(ship.get("MMSI", ""), {})
        if unit and not unit.get("home_base"):
            unit = enrich_unit(unit)
            ship["_unit"] = unit
        base = ship.get("HomeBase") or home_base_for_unit(unit)
        phase = ship.get("Phase", "home_port")

        lon0, lat0 = float(ship["Longitude"]), float(ship["Latitude"])
        if not is_baltic_water(lon0, lat0):
            lon, lat = snap_to_baltic_water(lon0, lat0)
            ship["Longitude"], ship["Latitude"] = lon, lat
            ship["RouteWaypoints"] = []
            ship["RouteIndex"] = 0

        if phase == "in_port":
            ticks = int(ship.get("PortStayTicksRemaining") or 0)
            if ticks > 0:
                ship["PortStayTicksRemaining"] = ticks - 1
                ship["Speed"] = 0.0
                ship["Status"] = "Moored"
                ship["Depth"] = 0.0
                continue
            ship["Phase"] = "home_port"
            _place_at_base(ship, base)
            if rng.random() < MISSION_ELIGIBLE_FRACTION:
                _start_mission(ship, unit, rng)
            continue

        if phase == "home_port":
            ship["Speed"] = 0.0
            ship["Status"] = "Anchored"
            ship["Depth"] = 0.0
            continue

        if phase == "anchored":
            rem = int(ship.get("AnchorTicksRemaining") or 0) - 1
            ship["AnchorTicksRemaining"] = rem
            ship["Speed"] = 0.0
            ship["Status"] = "Anchored"
            if rem <= 0:
                ship["Phase"] = "mission_patrol"
                ship["Speed"] = rng.uniform(6.0, 12.0)
                ship["Status"] = "Underway using engine"
            continue

        if phase in ("mission_patrol", "mission_return"):
            mt = int(ship.get("MissionTicksRemaining") or 0)
            if mt > 0:
                ship["MissionTicksRemaining"] = mt - 1
            elif phase == "mission_patrol":
                _begin_return_home(ship)
                phase = ship["Phase"]

        target = route_target(ship)
        if target is None and phase == "mission_return":
            _complete_mission_at_base(
                ship, float(ship["Latitude"]), float(ship["Longitude"]), base
            )
            continue
        if target is None:
            continue

        target_lat, target_lon = target
        new_lat, new_lon = move_towards_on_water(
            float(ship["Latitude"]),
            float(ship["Longitude"]),
            target_lat,
            target_lon,
            float(ship.get("Speed") or 8.0),
        )
        if is_baltic_water(new_lon, new_lat):
            ship["Latitude"] = new_lat
            ship["Longitude"] = new_lon

        lat, lon = float(ship["Latitude"]), float(ship["Longitude"])
        if abs(target_lat - lat) > 1e-6 or abs(target_lon - lon) > 1e-6:
            ship["Course"] = (
                math.degrees(math.atan2(target_lon - lon, target_lat - lat)) + 360
            ) % 360

        if phase == "mission_patrol":
            ship["Speed"] = max(
                4.0, float(ship.get("Speed") or 8) + rng.uniform(-SPEED_FLUCTUATION, SPEED_FLUCTUATION)
            )
            ship["Status"] = "Underway using engine"
            if unit.get("class") == "U-Boote":
                ship["Depth"] = random.uniform(10.0, 30.0)
            else:
                ship["Depth"] = random.uniform(5.0, 15.0)
            _advance_patrol_leg(ship, lat, lon, rng)
        elif phase == "mission_return":
            ship["Speed"] = max(
                4.0, float(ship.get("Speed") or 8) + rng.uniform(-SPEED_FLUCTUATION, SPEED_FLUCTUATION)
            )
            ship["Status"] = "Underway using engine"
            wps = ship.get("RouteWaypoints") or []
            idx = int(ship.get("RouteIndex", 0))
            if idx < len(wps):
                tgt_lat, tgt_lon = float(wps[idx]["lat"]), float(wps[idx]["lon"])
                if at_waypoint(lat, lon, tgt_lat, tgt_lon):
                    ship["RouteIndex"] = idx + 1
            if int(ship.get("RouteIndex", 0)) >= len(wps):
                _complete_mission_at_base(ship, lat, lon, base)

    return ships


def _hydrate_ship_from_json(ship: Dict[str, Any]) -> None:
    mmsi = ship.get("MMSI", "")
    if not ship.get("_unit") and mmsi in UNIT_BY_MMSI:
        ship["_unit"] = enrich_unit(UNIT_BY_MMSI[mmsi])
    if not ship.get("HomeBase"):
        unit = ship.get("_unit") or enrich_unit(UNIT_BY_MMSI.get(mmsi, {"class": "", "name": "", "mmsi": mmsi}))
        ship["HomeBase"] = home_base_for_unit(unit)
        ship["HomeBaseName"] = unit.get("home_base", "Kiel")
    dest = ship.get("Destination")
    if isinstance(dest, str):
        base = ship.get("HomeBase") or GERMAN_NAVAL_BASES["Kiel"]
        ship["Destination"] = dict(base)
    if not ship.get("Phase"):
        ship["Phase"] = "home_port"


def load_german_navy_state(
    num_ships: int,
    state_file: str = MARINESHIP_STATE_FILE,
    *,
    rng: Optional[random.Random] = None,
    operational_status_options: List[str],
    system_status_options: List[str],
) -> List[Dict[str, Any]]:
    if os.path.exists(state_file):
        with open(state_file, "r", encoding="utf-8") as f:
            ships = json.load(f)
        if not isinstance(ships, list):
            ships = []
        for ship in ships:
            if isinstance(ship, dict):
                _hydrate_ship_from_json(ship)
        if ships and all(isinstance(s, dict) and s.get("Phase") for s in ships):
            return ships[:num_ships] if len(ships) > num_ships else ships
    return initialize_german_navy_ships(
        num_ships,
        rng=rng,
        operational_status_options=operational_status_options,
        system_status_options=system_status_options,
    )


def save_german_navy_state(
    ships: List[Dict[str, Any]],
    state_file: str = MARINESHIP_STATE_FILE,
) -> None:
    out: List[Dict[str, Any]] = []
    for ship in ships:
        copy = dict(ship)
        copy.pop("_unit", None)
        if isinstance(copy.get("Destination"), dict):
            copy["Destination"] = copy["Destination"].get("name", "")
        if isinstance(copy.get("OriginPort"), dict):
            copy["OriginPort"] = copy["OriginPort"].get("name", copy.get("HomeBaseName", ""))
        if isinstance(copy.get("HomeBase"), dict):
            hb = copy.pop("HomeBase")
            if not copy.get("HomeBaseName"):
                copy["HomeBaseName"] = hb.get("name", "Kiel")
        out.append(copy)
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(out, f)


def count_mission_ships(ships: List[Dict[str, Any]]) -> int:
    return sum(
        1
        for s in ships
        if s.get("Phase") in ("mission_patrol", "mission_return", "anchored")
        or int(s.get("MissionTicksRemaining") or 0) > 0
    )
