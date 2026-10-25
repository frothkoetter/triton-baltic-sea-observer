"""Sea routes between harbors via water waypoints (no overland great-circle cuts)."""

from __future__ import annotations

import random
from typing import Any, Dict, List, Tuple

from pyproj import Geod

from baltic_water_placement import (
    OFFSHORE_SEEDS,
    is_baltic_water,
    sample_baltic_water_position,
    sample_water_near_harbor,
    snap_to_baltic_water,
)

_GEOD = Geod(ellps="WGS84")

# ~0.06° ≈ 3–4 nm — waypoint reached
WAYPOINT_ARRIVAL_DEG = 0.06

_MAX_SEED_HOP_M = 160_000
_MAX_ROUTE_HOPS = 28


def _route_waypoints_valid(waypoints: List[Dict[str, float]]) -> bool:
    if not waypoints:
        return False
    for wp in waypoints:
        if not is_baltic_water(float(wp["lon"]), float(wp["lat"])):
            return False
    return True


def _leg_samples_water(
    lon1: float,
    lat1: float,
    lon2: float,
    lat2: float,
    samples: int = 8,
) -> bool:
    """True if geodesic samples between two water points stay on water."""
    for i in range(1, samples):
        frac = i / samples
        lon = lon1 + frac * (lon2 - lon1)
        lat = lat1 + frac * (lat2 - lat1)
        if not is_baltic_water(lon, lat):
            return False
    return True


def build_sea_route(
    start_lon: float,
    start_lat: float,
    harbor_lon: float,
    harbor_lat: float,
    via_points: int = 5,
) -> List[Dict[str, float]]:
    """
    Water-only waypoints from start to an offshore point near ``harbor_*``.

    Uses greedy hops through ``OFFSHORE_SEEDS`` (progress toward goal), then
    final offshore anchorage — not a straight chord across land.
    """
    del via_points  # legacy arg; hop count is dynamic
    start_lon, start_lat = snap_to_baltic_water(start_lon, start_lat)
    end_lon, end_lat = sample_water_near_harbor(
        harbor_lon, harbor_lat, min_km=8.0, max_km=22.0
    )
    if not is_baltic_water(end_lon, end_lat):
        end_lon, end_lat = snap_to_baltic_water(end_lon, end_lat)

    path: List[Tuple[float, float]] = [(start_lon, start_lat)]
    current = (start_lon, start_lat)
    goal = (end_lon, end_lat)

    for _ in range(_MAX_ROUTE_HOPS):
        _, _, dist_goal_m = _GEOD.inv(current[0], current[1], goal[0], goal[1])
        if abs(dist_goal_m) <= 35_000:
            break

        best: Tuple[float, float, float] | None = None
        for slon, slat in OFFSHORE_SEEDS:
            if not is_baltic_water(slon, slat):
                continue
            _, _, d_curr = _GEOD.inv(current[0], current[1], slon, slat)
            if abs(d_curr) > _MAX_SEED_HOP_M:
                continue
            _, _, d_goal = _GEOD.inv(slon, slat, goal[0], goal[1])
            if abs(d_goal) >= abs(dist_goal_m) - 500.0:
                continue
            if not _leg_samples_water(current[0], current[1], slon, slat, samples=6):
                continue
            score = abs(d_goal) + 0.15 * abs(d_curr)
            if best is None or score < best[0]:
                best = (score, slon, slat)

        if best is None:
            break
        current = (best[1], best[2])
        if path[-1] != current:
            path.append(current)

    if path[-1] != goal:
        if _leg_samples_water(path[-1][0], path[-1][1], goal[0], goal[1], samples=10):
            path.append(goal)
        else:
            mid_lon = (path[-1][0] + goal[0]) / 2.0
            mid_lat = (path[-1][1] + goal[1]) / 2.0
            mid_lon, mid_lat = snap_to_baltic_water(mid_lon, mid_lat)
            if is_baltic_water(mid_lon, mid_lat):
                path.append((mid_lon, mid_lat))
            path.append(goal)

    waypoints = [{"lon": lon, "lat": lat} for lon, lat in path[1:]]
    if not _route_waypoints_valid(waypoints):
        return _fallback_dense_route(start_lon, start_lat, end_lon, end_lat)
    return waypoints


def _fallback_dense_route(
    start_lon: float,
    start_lat: float,
    end_lon: float,
    end_lat: float,
) -> List[Dict[str, float]]:
    """Chord densification with snap — last resort if seed routing fails."""
    _, _, dist_m = _GEOD.inv(start_lon, start_lat, end_lon, end_lat)
    steps = max(12, int(abs(dist_m) / 25_000) + 1)
    route: List[Dict[str, float]] = []
    for i in range(1, steps + 1):
        frac = i / steps
        lon = start_lon + frac * (end_lon - start_lon)
        lat = start_lat + frac * (end_lat - start_lat)
        lon, lat = snap_to_baltic_water(lon, lat)
        if is_baltic_water(lon, lat):
            route.append({"lon": lon, "lat": lat})
    if not route:
        lon, lat = sample_baltic_water_position()
        route.append({"lon": lon, "lat": lat})
    return route


def ensure_ship_route(ship: Dict[str, Any]) -> None:
    """Attach ``RouteWaypoints`` / ``RouteIndex`` if missing or invalid."""
    dest = ship.get("Destination")
    if not isinstance(dest, dict):
        return
    wps: List[Dict[str, float]] = ship.get("RouteWaypoints") or []
    if wps and _route_waypoints_valid(wps):
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
