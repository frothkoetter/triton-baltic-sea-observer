"""Pure simulation logic for Russian Baltic Fleet position updates (testable without NiFi)."""

from __future__ import annotations

import json
import math
import os
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from baltic_ship_routing import (
    SIMULATION_HOURS_PER_TICK,
    WAYPOINT_ARRIVAL_M,
    _GEOD,
    build_sea_route,
    move_towards_on_water,
    route_target,
)
from baltic_water_placement import is_baltic_water, sample_water_near_harbor, snap_to_baltic_water

from rus_fleet_registry import (
    MISSION_ELIGIBLE_FRACTION,
    PATROL_ZONE_SEEDS,
    RUS_FLEET_STATE_FILE,
    UNIT_BY_MMSI,
    fleet_units_for_simulation,
    home_base_for_unit,
)

# 7 days in port after mission (derived from routing tick length).
PORT_STAY_HOURS = 7 * 24
PORT_STAY_TICKS = int(PORT_STAY_HOURS / SIMULATION_HOURS_PER_TICK)

MISSION_MIN_DAYS = 14
MISSION_MAX_DAYS = 30
MISSION_MIN_TICKS = int(MISSION_MIN_DAYS * 24 / SIMULATION_HOURS_PER_TICK)
MISSION_MAX_TICKS = int(MISSION_MAX_DAYS * 24 / SIMULATION_HOURS_PER_TICK)

# Up to 24h anchored at patrol waypoint (simulated hours / tick).
ANCHOR_MAX_HOURS = 24
ANCHOR_MAX_TICKS = max(1, int(ANCHOR_MAX_HOURS / SIMULATION_HOURS_PER_TICK))
ANCHOR_PROBABILITY_PER_ARRIVAL = 0.22

SPEED_FLUCTUATION = 0.4

ShipPhase = str  # home_port | in_port | mission_patrol | mission_return | anchored


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


def _ticks_for_days(days: float) -> int:
    return max(1, int(days * 24 / SIMULATION_HOURS_PER_TICK))


def _random_mission_ticks(rng: random.Random) -> int:
    return rng.randint(MISSION_MIN_TICKS, MISSION_MAX_TICKS)


def _pick_patrol_loop(rng: random.Random, count: int = 4) -> List[Dict[str, Any]]:
    zones = list(PATROL_ZONE_SEEDS)
    rng.shuffle(zones)
    return [dict(z) for z in zones[: min(count, len(zones))]]


def _pseudo_destination(zone: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": zone["name"],
        "Latitude": float(zone["Latitude"]),
        "Longitude": float(zone["Longitude"]),
    }


def _place_at_home_base(
    ship: Dict[str, Any],
    base: Dict[str, Any],
    rng: random.Random,
) -> None:
    lon, lat = sample_water_near_harbor(
        float(base["Longitude"]),
        float(base["Latitude"]),
        min_km=0.5,
        max_km=4.0,
        seed=hash(ship.get("MMSI", "")) % 10_000,
    )
    ship["Latitude"] = lat
    ship["Longitude"] = lon
    ship["Speed"] = 0.0
    ship["Status"] = "Anchored"
    ship["Destination"] = dict(base)
    ship["RouteWaypoints"] = []
    ship["RouteIndex"] = 0


def _start_mission(ship: Dict[str, Any], rng: random.Random) -> None:
    ship["Phase"] = "mission_patrol"
    ship["MissionTicksRemaining"] = _random_mission_ticks(rng)
    ship["PatrolZones"] = _pick_patrol_loop(rng)
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
    ship["Speed"] = rng.uniform(8.0, 14.0)
    ship["Status"] = "Underway using engine"


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


def initialize_rus_fleet_ships(
    num_ships: int | None = None,
    *,
    rng: Optional[random.Random] = None,
) -> List[Dict[str, Any]]:
    rng = rng or random.Random()
    units = fleet_units_for_simulation(num_ships)
    ships: List[Dict[str, Any]] = []

    eligible = [u for u in units if u.get("default_mission_state") == "eligible_mission"]
    standby = [u for u in units if u.get("default_mission_state") != "eligible_mission"]
    target_mission = max(1, round(len(units) * MISSION_ELIGIBLE_FRACTION))
    mission_units = set()
    if eligible:
        rng.shuffle(eligible)
        mission_units.update(u["mmsi"] for u in eligible[:target_mission])
    remaining = target_mission - len(mission_units)
    if remaining > 0 and standby:
        rng.shuffle(standby)
        for u in standby[:remaining]:
            mission_units.add(u["mmsi"])

    for unit in units:
        base = home_base_for_unit(unit)
        ship: Dict[str, Any] = {
            "Name": unit["name"],
            "MMSI": unit["mmsi"],
            "Class": unit["class"],
            "VesselCategory": unit["vessel_category"],
            "HomeBaseName": unit["home_base"],
            "HomeBase": dict(base),
            "_unit": unit,
            "Latitude": base["Latitude"],
            "Longitude": base["Longitude"],
            "Speed": 0.0,
            "Course": rng.uniform(0, 360),
            "Status": "Anchored",
            "Destination": dict(base),
            "RouteWaypoints": [],
            "RouteIndex": 0,
            "Phase": "home_port",
            "MissionTicksRemaining": 0,
            "PortStayTicksRemaining": 0,
            "AnchorTicksRemaining": 0,
            "PatrolZones": [],
            "PatrolIndex": 0,
        }
        _place_at_home_base(ship, base, rng)
        if unit["mmsi"] in mission_units:
            _start_mission(ship, rng)
        ships.append(ship)
    return ships


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

    if rng.random() < ANCHOR_PROBABILITY_PER_ARRIVAL:
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


def _tick_mission_complete_at_home(ship: Dict[str, Any], lat: float, lon: float, base: Dict[str, Any]) -> None:
    if at_waypoint(lat, lon, float(base["Latitude"]), float(base["Longitude"]), threshold_m=8_000.0):
        ship["Phase"] = "in_port"
        ship["PortStayTicksRemaining"] = PORT_STAY_TICKS
        ship["MissionTicksRemaining"] = 0
        ship["Speed"] = 0.0
        ship["Status"] = "Moored"
        _place_at_home_base(ship, base, random.Random(0))


def update_rus_fleet_movements(
    ships: List[Dict[str, Any]],
    *,
    rng: Optional[random.Random] = None,
) -> List[Dict[str, Any]]:
    rng = rng or random.Random()
    for ship in ships:
        phase = ship.get("Phase", "home_port")
        base = ship.get("HomeBase") or home_base_for_unit(ship.get("_unit", {}))

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
                continue
            ship["Phase"] = "home_port"
            _place_at_home_base(ship, base, rng)
            if rng.random() < MISSION_ELIGIBLE_FRACTION:
                _start_mission(ship, rng)
            continue

        if phase == "home_port":
            ship["Speed"] = 0.0
            ship["Status"] = "Anchored"
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
            _tick_mission_complete_at_home(ship, float(ship["Latitude"]), float(ship["Longitude"]), base)
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
            ship["Speed"] = max(4.0, float(ship.get("Speed") or 8) + rng.uniform(-SPEED_FLUCTUATION, SPEED_FLUCTUATION))
            ship["Status"] = "Underway using engine"
            _advance_patrol_leg(ship, lat, lon, rng)
        elif phase == "mission_return":
            ship["Speed"] = max(4.0, float(ship.get("Speed") or 8) + rng.uniform(-SPEED_FLUCTUATION, SPEED_FLUCTUATION))
            ship["Status"] = "Underway using engine"
            wps = ship.get("RouteWaypoints") or []
            idx = int(ship.get("RouteIndex", 0))
            if idx < len(wps):
                tgt_lat, tgt_lon = float(wps[idx]["lat"]), float(wps[idx]["lon"])
                if at_waypoint(lat, lon, tgt_lat, tgt_lon):
                    ship["RouteIndex"] = idx + 1
            if int(ship.get("RouteIndex", 0)) >= len(wps):
                _tick_mission_complete_at_home(ship, lat, lon, base)

    return ships


def generate_rus_ais_message(ship: Dict[str, Any]) -> Dict[str, Any]:
    base = ship.get("HomeBase") or {}
    dest = ship.get("Destination") or {}
    return {
        "MMSI": ship["MMSI"],
        "message_from": ship.get("Name", ship["MMSI"]),
        "Event_Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "Latitude": round(float(ship["Latitude"]), 5),
        "Longitude": round(float(ship["Longitude"]), 5),
        "Speed": round(float(ship.get("Speed") or 0), 1),
        "Course": round(float(ship.get("Course") or 0), 1),
        "Status": ship.get("Status", "Unknown"),
        "Destination": dest.get("name", base.get("name", "")),
        "HomeBase": ship.get("HomeBaseName", base.get("name", "")),
        "Phase": ship.get("Phase", ""),
        "VesselCategory": ship.get("VesselCategory", ""),
    }


def load_rus_fleet_state(
    num_ships: int | None,
    state_file: str = RUS_FLEET_STATE_FILE,
    *,
    rng: Optional[random.Random] = None,
) -> List[Dict[str, Any]]:
    if os.path.exists(state_file):
        with open(state_file, "r", encoding="utf-8") as f:
            ships = json.load(f)
        for ship in ships:
            mmsi = ship.get("MMSI", "")
            if not ship.get("_unit") and mmsi in UNIT_BY_MMSI:
                ship["_unit"] = UNIT_BY_MMSI[mmsi]
            if not ship.get("HomeBase"):
                unit = ship.get("_unit") or {"home_base": ship.get("HomeBaseName", "Baltiysk")}
                ship["HomeBase"] = home_base_for_unit(unit)
            dest = ship.get("Destination")
            if isinstance(dest, str):
                ship["Destination"] = {"name": dest, "Latitude": 0.0, "Longitude": 0.0}
        return ships
    return initialize_rus_fleet_ships(num_ships, rng=rng)


def save_rus_fleet_state(
    ships: List[Dict[str, Any]],
    state_file: str = RUS_FLEET_STATE_FILE,
) -> None:
    out: List[Dict[str, Any]] = []
    for ship in ships:
        copy = {k: v for k, v in ship.items() if k != "_unit"}
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


def home_base_distribution(ships: List[Dict[str, Any]]) -> Dict[str, int]:
    dist: Dict[str, int] = {}
    for s in ships:
        if s.get("Phase") not in ("home_port", "in_port"):
            continue
        name = s.get("HomeBaseName") or (s.get("HomeBase") or {}).get("name", "unknown")
        dist[name] = dist.get(name, 0) + 1
    return dist
