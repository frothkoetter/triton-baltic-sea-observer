"""German fishing boat tour simulation (home port → low-speed grounds → same port, 12–36 h)."""

from __future__ import annotations

import json
import math
import os
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from baltic_ship_routing import (
    SIMULATION_HOURS_PER_TICK,
    WAYPOINT_ARRIVAL_M,
    build_sea_route,
    move_towards_on_water,
    route_target,
)
from baltic_water_placement import is_baltic_water, sample_water_near_harbor, snap_to_baltic_water

from fishing_fleet_registry import (
    FISHING_BOAT_STATE_FILE,
    GERMAN_FISHING_FLEET,
    NAME_BY_MMSI,
    UNIT_BY_MMSI,
    fleet_unit_for_index,
    home_port_for_unit,
)

TOUR_MIN_HOURS = 12
TOUR_MAX_HOURS = 36
TOUR_MIN_TICKS = int(TOUR_MIN_HOURS / SIMULATION_HOURS_PER_TICK)
TOUR_MAX_TICKS = int(TOUR_MAX_HOURS / SIMULATION_HOURS_PER_TICK)
PORT_STAY_MIN_TICKS = 3
PORT_STAY_MAX_TICKS = 8
AT_SEA_FRACTION_AT_INIT = 0.55
FISHING_SPEED_MIN_KN = 3.0
FISHING_SPEED_MAX_KN = 7.0
SPEED_FLUCTUATION_KN = 0.35


def at_waypoint(lat: float, lon: float, target_lat: float, target_lon: float) -> bool:
    from pyproj import Geod

    _, _, dist_m = Geod(ellps="WGS84").inv(lon, lat, target_lon, target_lat)
    return abs(dist_m) <= WAYPOINT_ARRIVAL_M


def _fishing_grounds_near_port(port: Dict[str, Any], mmsi: str, rng: random.Random) -> List[Dict[str, Any]]:
    """2–3 offshore waypoints 12–35 km from home (working area)."""
    base_lon = float(port["Longitude"])
    base_lat = float(port["Latitude"])
    count = rng.randint(2, 3)
    zones: List[Dict[str, Any]] = []
    for i in range(count):
        lon, lat = sample_water_near_harbor(
            base_lon,
            base_lat,
            min_km=12.0 + i * 4.0,
            max_km=22.0 + i * 8.0,
            seed=(hash(mmsi) + i * 997) % 10_000,
        )
        zones.append(
            {
                "name": f"Fishing ground {i + 1}",
                "Latitude": lat,
                "Longitude": lon,
            }
        )
    return zones


def _place_at_port(ship: Dict[str, Any], port: Dict[str, Any]) -> None:
    mmsi = ship.get("MMSI", "")
    lon, lat = sample_water_near_harbor(
        float(port["Longitude"]),
        float(port["Latitude"]),
        min_km=0.3,
        max_km=2.5,
        seed=hash(mmsi) % 10_000,
    )
    ship["Latitude"] = lat
    ship["Longitude"] = lon
    ship["Speed"] = 0.0
    ship["Status"] = "Moored"
    ship["Destination"] = dict(port)
    ship["RouteWaypoints"] = []
    ship["RouteIndex"] = 0
    ship["Depth"] = 0.0


def _begin_tour(ship: Dict[str, Any], unit: Dict[str, Any], rng: random.Random) -> None:
    port = ship.get("HomePort") or home_port_for_unit(unit)
    grounds = _fishing_grounds_near_port(port, str(unit.get("mmsi", "")), rng)
    ship["Phase"] = "tour_fishing"
    ship["TourTicksRemaining"] = rng.randint(TOUR_MIN_TICKS, TOUR_MAX_TICKS)
    ship["FishingGrounds"] = grounds
    ship["GroundIndex"] = 0
    ship["PortStayTicksRemaining"] = 0
    zone = grounds[0]
    ship["Destination"] = dict(zone)
    ship["RouteWaypoints"] = build_sea_route(
        float(ship["Longitude"]),
        float(ship["Latitude"]),
        float(zone["Longitude"]),
        float(zone["Latitude"]),
    )
    ship["RouteIndex"] = 0
    ship["Speed"] = rng.uniform(FISHING_SPEED_MIN_KN, FISHING_SPEED_MAX_KN)
    ship["Status"] = "Engaged in fishing"
    ship["Depth"] = rng.uniform(8.0, 22.0)


def _begin_return_home(ship: Dict[str, Any]) -> None:
    port = ship.get("HomePort") or {}
    ship["Phase"] = "tour_return"
    ship["Destination"] = dict(port)
    ship["RouteWaypoints"] = build_sea_route(
        float(ship["Longitude"]),
        float(ship["Latitude"]),
        float(port["Longitude"]),
        float(port["Latitude"]),
    )
    ship["RouteIndex"] = 0
    ship["Speed"] = min(float(ship.get("Speed") or 5.0), FISHING_SPEED_MAX_KN)
    ship["Status"] = "Under way to port"


def _advance_fishing_leg(ship: Dict[str, Any], lat: float, lon: float, rng: random.Random) -> None:
    grounds: List[Dict[str, Any]] = ship.get("FishingGrounds") or []
    if not grounds:
        _begin_return_home(ship)
        return
    idx = int(ship.get("GroundIndex", 0))
    if idx >= len(grounds):
        _begin_return_home(ship)
        return
    zone = grounds[idx]
    if at_waypoint(lat, lon, float(zone["Latitude"]), float(zone["Longitude"])):
        idx += 1
        ship["GroundIndex"] = idx
        if idx >= len(grounds):
            _begin_return_home(ship)
            return
        next_zone = grounds[idx]
        ship["Destination"] = dict(next_zone)
        ship["RouteWaypoints"] = build_sea_route(lon, lat, float(next_zone["Longitude"]), float(next_zone["Latitude"]))
        ship["RouteIndex"] = 0
        ship["Speed"] = rng.uniform(FISHING_SPEED_MIN_KN, FISHING_SPEED_MAX_KN)


def initialize_fishing_boats(
    num_boats: int,
    *,
    rng: Optional[random.Random] = None,
) -> List[Dict[str, Any]]:
    rng = rng or random.Random()
    n = min(max(1, num_boats), len(GERMAN_FISHING_FLEET))
    units = [fleet_unit_for_index(i) for i in range(n)]
    shuffled = list(units)
    rng.shuffle(shuffled)
    at_sea_count = max(1, round(len(units) * AT_SEA_FRACTION_AT_INIT))
    at_sea_mmsi = {u["mmsi"] for u in shuffled[:at_sea_count]}

    ships: List[Dict[str, Any]] = []
    for unit in units:
        port = home_port_for_unit(unit)
        ship: Dict[str, Any] = {
            "Name": unit["name"],
            "MMSI": unit["mmsi"],
            "HomePortName": unit["home_port"],
            "HomePort": dict(port),
            "VesselType": "Fishing",
            "_unit": unit,
            "Latitude": port["Latitude"],
            "Longitude": port["Longitude"],
            "Speed": 0.0,
            "Course": rng.uniform(0, 360),
            "Status": "Moored",
            "Destination": dict(port),
            "OriginPort": dict(port),
            "RouteWaypoints": [],
            "RouteIndex": 0,
            "Phase": "in_port",
            "TourTicksRemaining": 0,
            "PortStayTicksRemaining": 0,
            "FishingGrounds": [],
            "GroundIndex": 0,
            "Depth": 0.0,
        }
        _place_at_port(ship, port)
        if unit["mmsi"] in at_sea_mmsi:
            _begin_tour(ship, unit, rng)
        ships.append(ship)
    return ships


def update_fishing_boat_movements(
    ships: List[Dict[str, Any]],
    *,
    rng: Optional[random.Random] = None,
) -> List[Dict[str, Any]]:
    rng = rng or random.Random()
    for ship in ships:
        unit = ship.get("_unit") or UNIT_BY_MMSI.get(str(ship.get("MMSI", "")), {})
        port = ship.get("HomePort") or home_port_for_unit(unit)
        phase = ship.get("Phase", "in_port")

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
            if rng.random() < 0.65:
                _begin_tour(ship, unit, rng)
            continue

        if phase == "tour_fishing":
            tour_rem = int(ship.get("TourTicksRemaining") or 0) - 1
            ship["TourTicksRemaining"] = tour_rem
            if tour_rem <= 0:
                _begin_return_home(ship)
                phase = ship["Phase"]

        target = route_target(ship)
        if target is None and phase == "tour_return":
            if at_waypoint(
                float(ship["Latitude"]),
                float(ship["Longitude"]),
                float(port["Latitude"]),
                float(port["Longitude"]),
            ):
                ship["Phase"] = "in_port"
                ship["PortStayTicksRemaining"] = rng.randint(PORT_STAY_MIN_TICKS, PORT_STAY_MAX_TICKS)
                ship["TourTicksRemaining"] = 0
                ship["FishingGrounds"] = []
                ship["GroundIndex"] = 0
                _place_at_port(ship, port)
            continue
        if target is None:
            continue

        target_lat, target_lon = target
        speed = float(ship.get("Speed") or 5.0)
        new_lat, new_lon = move_towards_on_water(
            float(ship["Latitude"]),
            float(ship["Longitude"]),
            target_lat,
            target_lon,
            speed,
        )
        if is_baltic_water(new_lon, new_lat):
            ship["Latitude"] = new_lat
            ship["Longitude"] = new_lon

        lat, lon = float(ship["Latitude"]), float(ship["Longitude"])
        if abs(target_lat - lat) > 1e-6 or abs(target_lon - lon) > 1e-6:
            ship["Course"] = (
                math.degrees(math.atan2(target_lon - lon, target_lat - lat)) + 360
            ) % 360

        if phase == "tour_fishing":
            ship["Speed"] = max(
                FISHING_SPEED_MIN_KN,
                min(
                    FISHING_SPEED_MAX_KN,
                    speed + rng.uniform(-SPEED_FLUCTUATION_KN, SPEED_FLUCTUATION_KN),
                ),
            )
            ship["Status"] = "Engaged in fishing"
            ship["Depth"] = rng.uniform(10.0, 28.0)
            _advance_fishing_leg(ship, lat, lon, rng)
        elif phase == "tour_return":
            ship["Speed"] = max(
                FISHING_SPEED_MIN_KN,
                min(FISHING_SPEED_MAX_KN, speed + rng.uniform(-0.2, 0.2)),
            )
            ship["Status"] = "Under way to port"
            wps = ship.get("RouteWaypoints") or []
            idx = int(ship.get("RouteIndex", 0))
            if idx < len(wps):
                tgt_lat, tgt_lon = float(wps[idx]["lat"]), float(wps[idx]["lon"])
                if at_waypoint(lat, lon, tgt_lat, tgt_lon):
                    ship["RouteIndex"] = idx + 1
            if int(ship.get("RouteIndex", 0)) >= len(wps):
                ship["Phase"] = "in_port"
                ship["PortStayTicksRemaining"] = rng.randint(PORT_STAY_MIN_TICKS, PORT_STAY_MAX_TICKS)
                ship["TourTicksRemaining"] = 0
                _place_at_port(ship, port)

    return ships


def _hydrate_ship_from_json(ship: Dict[str, Any]) -> None:
    mmsi = str(ship.get("MMSI", ""))
    if not ship.get("_unit") and mmsi in UNIT_BY_MMSI:
        ship["_unit"] = dict(UNIT_BY_MMSI[mmsi])
    if not ship.get("HomePort"):
        unit = ship.get("_unit") or UNIT_BY_MMSI.get(mmsi, fleet_unit_for_index(0))
        ship["HomePort"] = home_port_for_unit(unit)
        ship["HomePortName"] = unit.get("home_port", "Rostock")
    if not ship.get("Phase"):
        ship["Phase"] = "in_port"


def load_fishing_boat_state(
    num_boats: int,
    state_file: str = FISHING_BOAT_STATE_FILE,
    *,
    rng: Optional[random.Random] = None,
) -> List[Dict[str, Any]]:
    if os.path.exists(state_file):
        with open(state_file, "r", encoding="utf-8") as f:
            ships = json.load(f)
        if isinstance(ships, list):
            for ship in ships:
                if isinstance(ship, dict):
                    _hydrate_ship_from_json(ship)
            if ships and all(isinstance(s, dict) and s.get("Phase") for s in ships):
                return ships[:num_boats] if len(ships) > num_boats else ships
    return initialize_fishing_boats(num_boats, rng=rng)


def save_fishing_boat_state(
    ships: List[Dict[str, Any]],
    state_file: str = FISHING_BOAT_STATE_FILE,
) -> None:
    out: List[Dict[str, Any]] = []
    for ship in ships:
        copy = dict(ship)
        copy.pop("_unit", None)
        if isinstance(copy.get("Destination"), dict):
            copy["Destination"] = copy["Destination"].get("name", "")
        if isinstance(copy.get("OriginPort"), dict):
            copy["OriginPort"] = copy["OriginPort"].get("name", copy.get("HomePortName", ""))
        if isinstance(copy.get("HomePort"), dict):
            hp = copy.pop("HomePort")
            if not copy.get("HomePortName"):
                copy["HomePortName"] = hp.get("name", "Rostock")
        out.append(copy)
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(out, f)


def count_at_sea(ships: List[Dict[str, Any]]) -> int:
    return sum(1 for s in ships if s.get("Phase") in ("tour_fishing", "tour_return"))


def generate_fishing_ais_message(ship: Dict[str, Any]) -> Dict[str, Any]:
    dest = ship.get("Destination") or {}
    dest_name = dest.get("name") if isinstance(dest, dict) else str(dest)
    home = ship.get("HomePortName") or ""
    origin = ship.get("OriginPort")
    if isinstance(origin, dict):
        origin_name = origin.get("name", home)
    else:
        origin_name = str(origin) if origin else home
    return {
        "MMSI": ship["MMSI"],
        "message_from": ship.get("Name") or NAME_BY_MMSI.get(str(ship["MMSI"]), ship["MMSI"]),
        "Event_Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "Latitude": round(float(ship["Latitude"]), 5),
        "Longitude": round(float(ship["Longitude"]), 5),
        "Speed": round(float(ship.get("Speed") or 0), 1),
        "Course": round(float(ship.get("Course") or 0), 1),
        "Status": ship.get("Status", "Unknown"),
        "Destination": dest_name,
        "OriginPort": origin_name,
        "HomePort": home,
        "Phase": ship.get("Phase", ""),
        "VesselType": "Fishing",
        "Depth": round(float(ship.get("Depth") or 0), 1),
    }
