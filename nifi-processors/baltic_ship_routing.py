"""Sea routes between harbors via water waypoints (no overland great-circle cuts)."""

from __future__ import annotations

import random
from typing import Any, Dict, List, Tuple

from baltic_water_placement import (
    is_baltic_water,
    sample_baltic_water_position,
    sample_water_near_harbor,
    snap_to_baltic_water,
)

# ~0.06° ≈ 3–4 nm — waypoint reached
WAYPOINT_ARRIVAL_DEG = 0.06


def build_sea_route(
    start_lon: float,
    start_lat: float,
    harbor_lon: float,
    harbor_lat: float,
    via_points: int = 3,
) -> List[Dict[str, float]]:
    """
    Water-only waypoints from start to an offshore point near ``harbor_*``.

    Intermediate points are snapped to water along the chord; ends are offshore.
    """
    start_lon, start_lat = snap_to_baltic_water(start_lon, start_lat)
    end_lon, end_lat = sample_water_near_harbor(
        harbor_lon, harbor_lat, min_km=8.0, max_km=22.0
    )
    route: List[Dict[str, float]] = []
    steps = max(1, via_points)
    for i in range(1, steps + 1):
        frac = i / (steps + 1)
        mid_lon = start_lon + frac * (end_lon - start_lon)
        mid_lat = start_lat + frac * (end_lat - start_lat)
        lon, lat = snap_to_baltic_water(mid_lon, mid_lat)
        if not is_baltic_water(lon, lat):
            lon, lat = sample_baltic_water_position()
        route.append({"lon": lon, "lat": lat})
    route.append({"lon": end_lon, "lat": end_lat})
    return route


def ensure_ship_route(ship: Dict[str, Any]) -> None:
    """Attach ``RouteWaypoints`` / ``RouteIndex`` if missing (state file migration)."""
    dest = ship.get("Destination")
    if not isinstance(dest, dict):
        return
    if ship.get("RouteWaypoints"):
        return
    ship["RouteWaypoints"] = build_sea_route(
        float(ship["Longitude"]),
        float(ship["Latitude"]),
        float(dest["Longitude"]),
        float(dest["Latitude"]),
    )
    ship["RouteIndex"] = 0


def route_target(ship: Dict[str, Any]) -> Tuple[float, float] | None:
    """Current (lat, lon) waypoint, or None if route is empty."""
    ensure_ship_route(ship)
    wps: List[Dict[str, float]] = ship.get("RouteWaypoints") or []
    idx = int(ship.get("RouteIndex", 0))
    if idx >= len(wps):
        return None
    wp = wps[idx]
    return float(wp["lat"]), float(wp["lon"])


def at_waypoint(lat: float, lon: float, target_lat: float, target_lon: float) -> bool:
    return (
        abs(lat - target_lat) < WAYPOINT_ARRIVAL_DEG
        and abs(lon - target_lon) < WAYPOINT_ARRIVAL_DEG
    )


def assign_new_destination(
    ship: Dict[str, Any],
    harbor: Dict[str, Any],
    *,
    depart: bool = True,
) -> None:
    """Set destination harbor and rebuild a water route from the current position."""
    ship["Destination"] = harbor
    ship["RouteWaypoints"] = build_sea_route(
        float(ship["Longitude"]),
        float(ship["Latitude"]),
        float(harbor["Longitude"]),
        float(harbor["Latitude"]),
    )
    ship["RouteIndex"] = 0
    if depart:
        ship["Status"] = "Underway using engine"
        ship["Speed"] = max(float(ship.get("Speed") or 0), random.uniform(4.0, 12.0))
    else:
        ship["Status"] = "Moored"
        ship["Speed"] = 0.0


def advance_route_or_arrive(
    ship: Dict[str, Any],
    harbors: List[Dict[str, Any]],
    lat: float,
    lon: float,
) -> bool:
    """
    If the ship reached the active waypoint, advance or pick a new harbor.

    Returns True if the ship arrived at the final waypoint (anchorage).
    """
    ensure_ship_route(ship)
    wps: List[Dict[str, float]] = ship["RouteWaypoints"]
    idx = int(ship.get("RouteIndex", 0))
    if idx >= len(wps):
        return True
    tgt_lat, tgt_lon = float(wps[idx]["lat"]), float(wps[idx]["lon"])
    if not at_waypoint(lat, lon, tgt_lat, tgt_lon):
        return False

    idx += 1
    ship["RouteIndex"] = idx
    if idx < len(wps):
        return False

    dest = ship["Destination"]
    dest_name = dest.get("name") if isinstance(dest, dict) else dest
    candidates = [h for h in harbors if h.get("name") != dest_name]
    new_harbor = random.choice(candidates) if candidates else random.choice(harbors)
    assign_new_destination(ship, new_harbor, depart=True)
    return True
