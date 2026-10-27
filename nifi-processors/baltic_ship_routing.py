"""Sea routes between harbors via water waypoints (no overland great-circle cuts)."""

from __future__ import annotations

import heapq
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

# One NiFi processor invocation ≈ this much simulated time (not wall clock).
SIMULATION_HOURS_PER_TICK = 5.0 / 60.0  # 5 minutes

# Waypoint reached when within this geodesic distance (m).
WAYPOINT_ARRIVAL_M = 2_500.0

_MAX_SEED_HOP_M = 160_000
_MAX_ROUTE_HOPS = 32
_LEG_SAMPLES = 16
_DENSIFY_STEP_M = 25_000
_NM_TO_M = 1852.0


def _route_waypoints_valid(waypoints: List[Dict[str, float]]) -> bool:
    if not waypoints:
        return False
    for wp in waypoints:
        if not is_baltic_water(float(wp["lon"]), float(wp["lat"])):
            return False
    return True


def leg_samples_water(
    lon1: float,
    lat1: float,
    lon2: float,
    lat2: float,
    samples: int | None = None,
) -> bool:
    """True if geodesic samples between two points stay on Baltic water."""
    az12, _, dist_m = _GEOD.inv(lon1, lat1, lon2, lat2)
    dist_m = abs(dist_m)
    if dist_m < 1.0:
        return is_baltic_water(lon2, lat2)
    if samples is None:
        samples = max(_LEG_SAMPLES, min(48, int(dist_m / 3_500) + 6))
    for i in range(1, samples):
        frac = i / samples
        lon, lat, _ = _GEOD.fwd(lon1, lat1, az12, dist_m * frac)
        if not is_baltic_water(lon, lat):
            return False
    return True


def route_path_valid(
    start_lon: float,
    start_lat: float,
    waypoints: List[Dict[str, float]],
) -> bool:
    if not _route_waypoints_valid(waypoints):
        return False
    prev_lon, prev_lat = start_lon, start_lat
    for wp in waypoints:
        wlon, wlat = float(wp["lon"]), float(wp["lat"])
        if not leg_samples_water(prev_lon, prev_lat, wlon, wlat):
            return False
        prev_lon, prev_lat = wlon, wlat
    return True


def densify_route(
    start_lon: float,
    start_lat: float,
    waypoints: List[Dict[str, float]],
    step_m: float = _DENSIFY_STEP_M,
) -> List[Dict[str, float]]:
    """Insert geodesic points along each leg for map polylines (~step_m apart)."""
    if not waypoints:
        return []
    dense: List[Dict[str, float]] = []
    prev_lon, prev_lat = start_lon, start_lat
    for wp in waypoints:
        wlon, wlat = float(wp["lon"]), float(wp["lat"])
        az12, _, dist_m = _GEOD.inv(prev_lon, prev_lat, wlon, wlat)
        dist_m = abs(dist_m)
        steps = max(1, int(dist_m / step_m))
        for i in range(1, steps + 1):
            frac = i / steps
            lon, lat, _ = _GEOD.fwd(prev_lon, prev_lat, az12, dist_m * frac)
            if is_baltic_water(lon, lat):
                dense.append({"lon": float(lon), "lat": float(lat)})
        prev_lon, prev_lat = wlon, wlat
    return dense


def move_geodesic_knots(
    lon1: float,
    lat1: float,
    lon2: float,
    lat2: float,
    speed_knots: float,
    hours: float = 1.0,
) -> Tuple[float, float]:
    """One simulation tick: move from (lon1,lat1) toward (lon2,lat2) at ``speed_knots``."""
    az12, _, dist_m = _GEOD.inv(lon1, lat1, lon2, lat2)
    dist_m = abs(dist_m)
    if dist_m < 1.0:
        return lat2, lon2
    step_m = min(dist_m, max(0.0, speed_knots) * _NM_TO_M * hours)
    lon, lat, _ = _GEOD.fwd(lon1, lat1, az12, step_m)
    return float(lat), float(lon)


def move_towards_on_water(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
    speed_knots: float,
    substeps: int = 8,
    hours: float | None = None,
) -> Tuple[float, float]:
    """Step toward the waypoint along geodesics; never snap across land."""
    lat, lon = lat1, lon1
    tick_h = hours if hours is not None else SIMULATION_HOURS_PER_TICK
    step_hours = tick_h / max(1, substeps)
    for _ in range(substeps):
        nlat, nlon = move_geodesic_knots(
            lon, lat, lon2, lat2, speed_knots, hours=step_hours
        )
        if is_baltic_water(nlon, nlat):
            lat, lon = nlat, nlon
        else:
            break
    return lat, lon


def _simplify_water_path(path: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
    """Drop intermediate nodes when a direct geodesic leg stays on water (fewer zig-zags)."""
    if len(path) <= 2:
        return path
    out: List[Tuple[float, float]] = [path[0]]
    i = 0
    while i < len(path) - 1:
        j = len(path) - 1
        while j > i + 1:
            if leg_samples_water(path[i][0], path[i][1], path[j][0], path[j][1]):
                out.append(path[j])
                i = j
                break
            j -= 1
        else:
            out.append(path[i + 1])
            i += 1
    return out


def _water_graph_path(
    start_lon: float,
    start_lat: float,
    goal_lon: float,
    goal_lat: float,
) -> List[Tuple[float, float]]:
    """Shortest water-only path via offshore seeds (Dijkstra on validated edges)."""
    nodes: List[Tuple[float, float]] = [(start_lon, start_lat)]
    for slon, slat in OFFSHORE_SEEDS:
        if is_baltic_water(slon, slat):
            nodes.append((slon, slat))
    nodes.append((goal_lon, goal_lat))
    n = len(nodes)
    goal_i = n - 1
    adj: List[List[Tuple[int, float]]] = [[] for _ in range(n)]
    for i in range(n):
        lon1, lat1 = nodes[i]
        for j in range(i + 1, n):
            lon2, lat2 = nodes[j]
            _, _, dist_m = _GEOD.inv(lon1, lat1, lon2, lat2)
            if abs(dist_m) > _MAX_SEED_HOP_M:
                continue
            if not leg_samples_water(lon1, lat1, lon2, lat2):
                continue
            d = abs(dist_m)
            adj[i].append((j, d))
            adj[j].append((i, d))

    dist = [1e18] * n
    dist[0] = 0.0
    prev = [-1] * n
    heap: List[Tuple[float, int]] = [(0.0, 0)]
    while heap:
        d_u, u = heapq.heappop(heap)
        if d_u > dist[u]:
            continue
        if u == goal_i:
            break
        for v, w in adj[u]:
            nd = d_u + w
            if nd < dist[v]:
                dist[v] = nd
                prev[v] = u
                heapq.heappush(heap, (nd, v))

    if dist[goal_i] >= 1e17:
        return []

    path: List[Tuple[float, float]] = []
    cur = goal_i
    while cur >= 0:
        path.append(nodes[cur])
        cur = prev[cur]
    path.reverse()
    return _simplify_water_path(path)


def build_sea_route(
    start_lon: float,
    start_lat: float,
    harbor_lon: float,
    harbor_lat: float,
    via_points: int = 5,
) -> List[Dict[str, float]]:
    """
    Water-only waypoints from start to an offshore point near ``harbor_*``.

    Shortest path through ``OFFSHORE_SEEDS`` with geodesic leg checks (no land cuts).
    """
    del via_points  # legacy arg
    start_lon, start_lat = snap_to_baltic_water(start_lon, start_lat)
    end_lon, end_lat = sample_water_near_harbor(
        harbor_lon, harbor_lat, min_km=8.0, max_km=22.0
    )
    if not is_baltic_water(end_lon, end_lat):
        end_lon, end_lat = snap_to_baltic_water(end_lon, end_lat)

    path = _water_graph_path(start_lon, start_lat, end_lon, end_lat)
    sparse = [{"lon": lon, "lat": lat} for lon, lat in path[1:]]
    if not route_path_valid(start_lon, start_lat, sparse):
        sparse = _fallback_dense_route(start_lon, start_lat, end_lon, end_lat)
    if not route_path_valid(start_lon, start_lat, sparse):
        end_lon, end_lat = sample_water_near_harbor(harbor_lon, harbor_lat, 5.0, 18.0, seed=42)
        path = _water_graph_path(start_lon, start_lat, end_lon, end_lat)
        sparse = [{"lon": lon, "lat": lat} for lon, lat in path[1:]]
    if not route_path_valid(start_lon, start_lat, sparse):
        sparse = _fallback_dense_route(start_lon, start_lat, end_lon, end_lat)

    # Sparse waypoints only — densify_route() is for map polylines / export, not ship ticks.
    return sparse


def _fallback_dense_route(
    start_lon: float,
    start_lat: float,
    end_lon: float,
    end_lat: float,
) -> List[Dict[str, float]]:
    """Geodesic chord densification with snap — only keeps water samples on each leg."""
    _, _, dist_m = _GEOD.inv(start_lon, start_lat, end_lon, end_lat)
    steps = max(16, int(abs(dist_m) / 20_000) + 1)
    az12, _, total_m = _GEOD.inv(start_lon, start_lat, end_lon, end_lat)
    total_m = abs(total_m)
    route: List[Dict[str, float]] = []
    for i in range(1, steps + 1):
        frac = i / steps
        lon, lat, _ = _GEOD.fwd(start_lon, start_lat, az12, total_m * frac)
        lon, lat = snap_to_baltic_water(lon, lat)
        if is_baltic_water(lon, lat):
            route.append({"lon": lon, "lat": lat})
    if not route:
        lon, lat = sample_baltic_water_position()
        route.append({"lon": lon, "lat": lat})
    return route


def _trim_passed_waypoints(ship: Dict[str, Any]) -> None:
    """Drop waypoints already behind the vessel (avoids snapping back to wp[0])."""
    wps: List[Dict[str, float]] = ship.get("RouteWaypoints") or []
    if not wps:
        return
    idx = int(ship.get("RouteIndex", 0))
    if idx > 0:
        ship["RouteWaypoints"] = wps[idx:]
        ship["RouteIndex"] = 0
        return
    lon = float(ship["Longitude"])
    lat = float(ship["Latitude"])
    while len(wps) > 1:
        wlon, wlat = float(wps[0]["lon"]), float(wps[0]["lat"])
        if not at_waypoint(lat, lon, wlat, wlon):
            break
        wps = wps[1:]
    ship["RouteWaypoints"] = wps
    ship["RouteIndex"] = 0


def ensure_ship_route(ship: Dict[str, Any]) -> None:
    """Attach ``RouteWaypoints`` / ``RouteIndex`` if missing or invalid."""
    dest = ship.get("Destination")
    if not isinstance(dest, dict):
        return
    start_lon = float(ship["Longitude"])
    start_lat = float(ship["Latitude"])
    _trim_passed_waypoints(ship)
    wps: List[Dict[str, float]] = ship.get("RouteWaypoints") or []
    idx = int(ship.get("RouteIndex", 0))
    remaining = wps[idx:] if idx < len(wps) else []
    if remaining and route_path_valid(start_lon, start_lat, remaining):
        if idx > 0:
            ship["RouteWaypoints"] = remaining
            ship["RouteIndex"] = 0
        return
    ship["RouteWaypoints"] = build_sea_route(
        start_lon,
        start_lat,
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
    _, _, dist_m = _GEOD.inv(lon, lat, target_lon, target_lat)
    return abs(dist_m) <= WAYPOINT_ARRIVAL_M


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
    origin = ship.get("OriginPort")
    if isinstance(origin, dict) and origin.get("name") == new_harbor.get("name"):
        others = [h for h in candidates if h.get("name") != origin.get("name")]
        if others:
            new_harbor = random.choice(others)
    if isinstance(dest, dict):
        ship["OriginPort"] = dest
    assign_new_destination(ship, new_harbor, depart=True)
    return True
