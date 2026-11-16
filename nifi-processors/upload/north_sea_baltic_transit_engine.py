"""North Sea ↔ Baltic trade cycle with moored fast-path (1 NiFi tick = 1 sim hour)."""

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
    _destination_key,
    at_waypoint,
    build_sea_route,
    move_towards_on_water,
    route_target,
    warm_routing_graph,
)
from baltic_water_placement import harbor_departure_position, is_baltic_water, sample_water_near_harbor

from north_sea_baltic_fleet_registry import (
    BALTIC_BY_NAME,
    BALTIC_TRADE_PORTS,
    HUB_BY_NAME,
    NSB_TRADE_STATE_FILE,
    NORTH_SEA_HUBS,
    fleet_unit_for_index,
)

SPEED_FLUCT_KN = 0.4
MOORED_PHASES = frozenset({"moored_ns", "moored_baltic"})
TRANSIT_PHASES = frozenset({"transit_to_baltic", "transit_to_ns"})

# Shared sea routes for identical hub↔port pairs (major init / tick-1 speedup at 1000+ ships).
_PAIR_ROUTE_CACHE: Dict[Tuple[str, str], List[Dict[str, float]]] = {}
_PAIR_ROUTES_WARMED = False

# Loose North Sea / southern North Sea boxes (movement allowed outside Baltic mask).
_NORTH_SEA_BOXES = (
    (3.2, 12.2, 51.0, 54.2),
    (9.0, 12.4, 53.5, 54.6),
)


def is_corridor_water(lon: float, lat: float) -> bool:
    if is_baltic_water(lon, lat):
        return True
    for lon_min, lon_max, lat_min, lat_max in _NORTH_SEA_BOXES:
        if lon_min <= lon <= lon_max and lat_min <= lat <= lat_max:
            return True
    return False


def _rng_for_mmsi(mmsi: str) -> random.Random:
    return random.Random(hash(mmsi) % (2**32))


def _port_dict(name: str) -> Dict[str, float | str]:
    if name in HUB_BY_NAME:
        return dict(HUB_BY_NAME[name])
    if name in BALTIC_BY_NAME:
        return dict(BALTIC_BY_NAME[name])
    return dict(BALTIC_TRADE_PORTS[0])


def _moor_at(ship: Dict[str, Any], port: Dict[str, Any]) -> None:
    mmsi = str(ship.get("MMSI", ""))
    lon, lat = sample_water_near_harbor(
        float(port["Longitude"]),
        float(port["Latitude"]),
        min_km=0.4,
        max_km=2.8,
        seed=hash(mmsi) % 10_000,
    )
    ship["Latitude"] = lat
    ship["Longitude"] = lon
    ship["Speed"] = 0.0
    ship["Status"] = "Moored"
    ship["Destination"] = dict(port)
    ship["OriginPort"] = dict(port)
    ship["RouteWaypoints"] = []
    ship["RouteIndex"] = 0
    ship.pop("RouteDestKey", None)


def _pair_route(origin_name: str, dest_name: str) -> List[Dict[str, float]]:
    key = (origin_name, dest_name)
    cached = _PAIR_ROUTE_CACHE.get(key)
    if cached is not None:
        return [dict(wp) for wp in cached]
    origin = _port_dict(origin_name)
    dest = _port_dict(dest_name)
    o_lon, o_lat = harbor_departure_position(
        float(origin["Longitude"]),
        float(origin["Latitude"]),
    )
    route = build_sea_route(
        o_lon,
        o_lat,
        float(dest["Longitude"]),
        float(dest["Latitude"]),
    )
    if route:
        _PAIR_ROUTE_CACHE[key] = [dict(wp) for wp in route]
        return [dict(wp) for wp in route]
    return []


def _set_transit_route(ship: Dict[str, Any], dest: Dict[str, Any]) -> bool:
    phase = ship.get("Phase", "")
    if phase == "transit_to_baltic":
        route = _pair_route(
            str(ship.get("NorthSeaHubName") or ""),
            str(dest.get("name") or ship.get("BalticPortName") or ""),
        )
    elif phase == "transit_to_ns":
        route = _pair_route(
            str(ship.get("BalticPortName") or ""),
            str(dest.get("name") or ship.get("NorthSeaHubName") or ""),
        )
    else:
        route = build_sea_route(
            float(ship["Longitude"]),
            float(ship["Latitude"]),
            float(dest["Longitude"]),
            float(dest["Latitude"]),
        )
    if not route:
        return False
    ship["Destination"] = dict(dest)
    ship["RouteWaypoints"] = route
    ship["RouteIndex"] = 0
    ship["RouteDestKey"] = _destination_key(dest)
    ship["Status"] = "Underway using engine"
    unit = ship.get("_unit") or {}
    base = float(unit.get("speed_base_kn") or ship.get("Speed") or 12.0)
    ship["Speed"] = max(6.0, base + random.uniform(-1.0, 1.5))
    return True


def _begin_transit_to_baltic(ship: Dict[str, Any]) -> None:
    dest = _port_dict(str(ship.get("BalticPortName") or ""))
    ship["Phase"] = "transit_to_baltic"
    ship["DwellHoursRemaining"] = 0
    if not _set_transit_route(ship, dest):
        _moor_at(ship, dest)
        ship["Phase"] = "moored_baltic"
        ship["DwellHoursRemaining"] = float(
            (ship.get("_unit") or {}).get("dwell_baltic_hours") or 24
        )


def _begin_transit_to_north_sea(ship: Dict[str, Any]) -> None:
    dest = _port_dict(str(ship.get("NorthSeaHubName") or ""))
    ship["Phase"] = "transit_to_ns"
    ship["DwellHoursRemaining"] = 0
    if not _set_transit_route(ship, dest):
        _moor_at(ship, dest)
        ship["Phase"] = "moored_ns"
        ship["DwellHoursRemaining"] = float(
            (ship.get("_unit") or {}).get("dwell_ns_hours") or 18
        )


def _on_transit_arrival(ship: Dict[str, Any]) -> None:
    phase = ship.get("Phase")
    unit = ship.get("_unit") or {}
    if phase == "transit_to_baltic":
        port = _port_dict(str(ship.get("BalticPortName") or ""))
        ship["Phase"] = "moored_baltic"
        ship["DwellHoursRemaining"] = float(unit.get("dwell_baltic_hours") or 24)
        _moor_at(ship, port)
    elif phase == "transit_to_ns":
        port = _port_dict(str(ship.get("NorthSeaHubName") or ""))
        ship["Phase"] = "moored_ns"
        ship["DwellHoursRemaining"] = float(unit.get("dwell_ns_hours") or 18)
        _moor_at(ship, port)


def _advance_waypoints(ship: Dict[str, Any], lat: float, lon: float) -> bool:
    """Returns True when the vessel reached the final waypoint (destination)."""
    wps: List[Dict[str, float]] = ship.get("RouteWaypoints") or []
    idx = int(ship.get("RouteIndex", 0))
    if idx >= len(wps):
        return True
    tgt_lat, tgt_lon = float(wps[idx]["lat"]), float(wps[idx]["lon"])
    if not at_waypoint(lat, lon, tgt_lat, tgt_lon):
        return False
    idx += 1
    ship["RouteIndex"] = idx
    return idx >= len(wps)


def warm_nsb_pair_routes() -> None:
    """Precompute hub↔Baltic leg templates once per process (96×2 directed pairs max)."""
    global _PAIR_ROUTES_WARMED
    if _PAIR_ROUTES_WARMED:
        return
    warm_routing_graph()
    for hub in NORTH_SEA_HUBS:
        hn = str(hub["name"])
        for baltic in BALTIC_TRADE_PORTS:
            bn = str(baltic["name"])
            _pair_route(hn, bn)
            _pair_route(bn, hn)
    _PAIR_ROUTES_WARMED = True


def initialize_nsb_trade_ships(num_ships: int) -> List[Dict[str, Any]]:
    n = min(max(1, num_ships), 2000)
    ships: List[Dict[str, Any]] = []
    for i in range(n):
        unit = fleet_unit_for_index(i)
        ns = _port_dict(unit["north_sea_hub"])
        baltic = _port_dict(unit["baltic_port"])
        rng = _rng_for_mmsi(unit["mmsi"])
        roll = rng.random()
        if roll < 0.34:
            phase = "moored_ns"
        elif roll < 0.68:
            phase = "moored_baltic"
        elif roll < 0.84:
            phase = "transit_to_baltic"
        else:
            phase = "transit_to_ns"

        ship: Dict[str, Any] = {
            "Name": unit["name"],
            "MMSI": unit["mmsi"],
            "VesselType": unit["vessel_type"],
            "NorthSeaHubName": unit["north_sea_hub"],
            "BalticPortName": unit["baltic_port"],
            "_unit": unit,
            "Phase": phase,
            "DwellHoursRemaining": float(
                unit["dwell_ns_hours"] if phase == "moored_ns" else unit["dwell_baltic_hours"]
            ),
            "Latitude": ns["Latitude"],
            "Longitude": ns["Longitude"],
            "Speed": 0.0,
            "Course": rng.uniform(0, 360),
            "Status": "Moored",
            "Destination": dict(ns if phase.endswith("_ns") or phase == "transit_to_ns" else baltic),
            "OriginPort": dict(ns),
            "RouteWaypoints": [],
            "RouteIndex": 0,
        }
        if phase == "moored_ns":
            _moor_at(ship, ns)
        elif phase == "moored_baltic":
            _moor_at(ship, baltic)
        elif phase == "transit_to_baltic":
            lon, lat = harbor_departure_position(float(ns["Longitude"]), float(ns["Latitude"]))
            ship["Longitude"], ship["Latitude"] = lon, lat
            ship["Destination"] = dict(baltic)
            ship["RouteWaypoints"] = []
        else:
            lon, lat = harbor_departure_position(float(baltic["Longitude"]), float(baltic["Latitude"]))
            ship["Longitude"], ship["Latitude"] = lon, lat
            ship["Destination"] = dict(ns)
            ship["RouteWaypoints"] = []
        ships.append(ship)
    return ships


def _tick_moored(ship: Dict[str, Any]) -> None:
    dwell = float(ship.get("DwellHoursRemaining") or 0) - SIMULATION_HOURS_PER_TICK
    ship["DwellHoursRemaining"] = dwell
    ship["Speed"] = 0.0
    ship["Status"] = "Moored"
    if dwell > 0:
        return
    phase = ship.get("Phase")
    if phase == "moored_ns":
        _begin_transit_to_baltic(ship)
    elif phase == "moored_baltic":
        _begin_transit_to_north_sea(ship)


def _tick_transit(ship: Dict[str, Any], rng: random.Random) -> None:
    if not ship.get("RouteWaypoints"):
        phase = ship.get("Phase")
        if phase == "transit_to_baltic":
            _set_transit_route(ship, _port_dict(str(ship.get("BalticPortName") or "")))
        elif phase == "transit_to_ns":
            _set_transit_route(ship, _port_dict(str(ship.get("NorthSeaHubName") or "")))
        if not ship.get("RouteWaypoints"):
            _on_transit_arrival(ship)
            return

    target = route_target(ship)
    if target is None:
        _on_transit_arrival(ship)
        return

    target_lat, target_lon = target
    speed = float(ship.get("Speed") or 12.0)
    new_lat, new_lon = move_towards_on_water(
        float(ship["Latitude"]),
        float(ship["Longitude"]),
        target_lat,
        target_lon,
        speed,
    )
    if is_corridor_water(new_lon, new_lat):
        ship["Latitude"] = new_lat
        ship["Longitude"] = new_lon

    lat, lon = float(ship["Latitude"]), float(ship["Longitude"])
    if abs(target_lat - lat) > 1e-6 or abs(target_lon - lon) > 1e-6:
        ship["Course"] = (
            math.degrees(math.atan2(target_lon - lon, target_lat - lat)) + 360
        ) % 360
    unit = ship.get("_unit") or {}
    base = float(unit.get("speed_base_kn") or speed)
    ship["Speed"] = max(4.0, base + rng.uniform(-SPEED_FLUCT_KN, SPEED_FLUCT_KN))
    ship["Status"] = random.choice(["Underway using engine", "Underway"])

    if _advance_waypoints(ship, lat, lon):
        _on_transit_arrival(ship)


def update_nsb_trade_movements(ships: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for ship in ships:
        phase = ship.get("Phase", "moored_ns")
        if phase in MOORED_PHASES:
            _tick_moored(ship)
            continue
        if phase in TRANSIT_PHASES:
            _tick_transit(ship, _rng_for_mmsi(str(ship.get("MMSI", ""))))
    return ships


def generate_nsb_ais_message(ship: Dict[str, Any]) -> Dict[str, Any]:
    dest = ship.get("Destination") or {}
    dest_name = dest.get("name") if isinstance(dest, dict) else str(dest)
    return {
        "MMSI": ship["MMSI"],
        "message_from": ship.get("Name") or ship["MMSI"],
        "Event_Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "Latitude": round(float(ship["Latitude"]), 5),
        "Longitude": round(float(ship["Longitude"]), 5),
        "Speed": round(float(ship.get("Speed") or 0), 1),
        "Course": round(float(ship.get("Course") or 0), 1),
        "Status": ship.get("Status", "Unknown"),
        "Destination": dest_name,
        "OriginPort": ship.get("NorthSeaHubName", ""),
        "BalticPort": ship.get("BalticPortName", ""),
        "Phase": ship.get("Phase", ""),
        "VesselType": ship.get("VesselType", ""),
    }


def _hydrate_port_names(ship: Dict[str, Any]) -> None:
    dest = ship.get("Destination")
    if isinstance(dest, str):
        ship["Destination"] = _port_dict(dest)
    origin = ship.get("OriginPort")
    if isinstance(origin, str):
        ship["OriginPort"] = _port_dict(origin)
    mmsi = str(ship.get("MMSI", ""))
    if "_unit" not in ship:
        try:
            slot = int(mmsi) - 311_040_001
            if slot >= 0:
                ship["_unit"] = fleet_unit_for_index(slot)
        except ValueError:
            pass


def load_nsb_trade_state(num_ships: int) -> List[Dict[str, Any]]:
    if os.path.exists(NSB_TRADE_STATE_FILE):
        with open(NSB_TRADE_STATE_FILE, "r", encoding="utf-8") as f:
            ships = json.load(f)
        if not isinstance(ships, list):
            return initialize_nsb_trade_ships(num_ships)
        for ship in ships:
            if isinstance(ship, dict):
                _hydrate_port_names(ship)
        if len(ships) != num_ships:
            return initialize_nsb_trade_ships(num_ships)
        return ships
    return initialize_nsb_trade_ships(num_ships)


def save_nsb_trade_state(ships: List[Dict[str, Any]]) -> None:
    out: List[Dict[str, Any]] = []
    for ship in ships:
        copy = {k: v for k, v in ship.items() if k != "_unit"}
        dest = copy.get("Destination")
        if isinstance(dest, dict):
            copy["Destination"] = dest.get("name", "")
        origin = copy.get("OriginPort")
        if isinstance(origin, dict):
            copy["OriginPort"] = origin.get("name", "")
        out.append(copy)
    with open(NSB_TRADE_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f)


if __name__ == "__main__":
    ships = initialize_nsb_trade_ships(5)
    ships = update_nsb_trade_movements(ships)
    for s in ships:
        print(generate_nsb_ais_message(s))