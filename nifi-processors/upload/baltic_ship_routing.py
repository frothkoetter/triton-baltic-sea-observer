"""Sea routes between harbors via water waypoints (no overland great-circle cuts)."""

from __future__ import annotations

import heapq
import random
from typing import Any, Dict, List, Tuple

from pyproj import Geod

from baltic_water_placement import (
    CHANNEL_BOXES,
    OFFSHORE_SEEDS,
    is_baltic_water,
    sample_baltic_water_position,
    sample_water_near_harbor,
    snap_to_baltic_water,
)

_GEOD = Geod(ellps="WGS84")

# One NiFi GenerateFlowFile tick (typically 1 min wall clock) advances this much simulated time.
SIMULATION_HOURS_PER_TICK = 1.0  # 1 hour

# Waypoint reached when within this geodesic distance (m).
WAYPOINT_ARRIVAL_M = 2_500.0

_MAX_SEED_HOP_M = 320_000  # ~173 nm; Gedser/Arkona + cross-basin seed hops
_BFS_MAX_HOP_M = 320_000
_STATIC_GRAPH_MAX_EDGE_M = 120_000  # fairway chains; still spans Irbe–GoF ladder hops
_HARBOR_APPROACH_SAMPLES = 6
_STEP_ROUTE_M = 22_000
_MAX_STEP_ROUTE_STEPS = 72
_MAX_ROUTE_HOPS = 32
_LEG_SAMPLES = 16
_DENSIFY_STEP_M = 25_000
_NM_TO_M = 1852.0

# Cached sea routes between coarse start/goal cells (routing is expensive).
_ROUTE_CACHE: Dict[Tuple[float, float, float, float], List[Dict[str, float]]] = {}
_ROUTE_CACHE_MAX = 512

_ROUTING_SEEDS: List[Tuple[float, float]] | None = None
_MAIN_SEED_IDX: set[int] | None = None


def _routing_seed_list() -> List[Tuple[float, float]]:
    global _ROUTING_SEEDS
    if _ROUTING_SEEDS is None:
        _ROUTING_SEEDS = [
            (float(slon), float(slat))
            for slon, slat in OFFSHORE_SEEDS
            if is_baltic_water(float(slon), float(slat))
        ]
    return _ROUTING_SEEDS


def _main_routing_seed_indexes() -> set[int]:
    """Indexes of OFFSHORE_SEEDS in the largest water-only routing component."""
    global _MAIN_SEED_IDX
    if _MAIN_SEED_IDX is not None:
        return _MAIN_SEED_IDX

    seeds = _routing_seed_list()
    n = len(seeds)
    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for i in range(n):
        lon1, lat1 = seeds[i]
        for j in range(i + 1, n):
            lon2, lat2 = seeds[j]
            _, _, dist_m = _GEOD.inv(lon1, lat1, lon2, lat2)
            if abs(dist_m) > _MAX_SEED_HOP_M:
                continue
            if leg_samples_water(lon1, lat1, lon2, lat2):
                union(i, j)

    from collections import Counter

    roots = Counter(find(i) for i in range(n))
    main_root = roots.most_common(1)[0][0]
    _MAIN_SEED_IDX = {i for i in range(n) if find(i) == main_root}
    return _MAIN_SEED_IDX


def _anchor_routing_start(
    start_lon: float,
    start_lat: float,
) -> Tuple[float, float, List[Dict[str, float]]]:
    """
    If ``start`` sits in a small water pocket, prepend a leg to the main seed graph.
    Returns (route_start_lon, route_start_lat, prefix_waypoints).
    """
    lon, lat = snap_to_baltic_water(start_lon, start_lat)
    seeds = _routing_seed_list()
    main = _main_routing_seed_indexes()

    best: Tuple[float, float, float] | None = None
    for i in main:
        slon, slat = seeds[i]
        if not leg_samples_water(lon, lat, slon, slat):
            continue
        _, _, dist_m = _GEOD.inv(lon, lat, slon, slat)
        dist_m = abs(dist_m)
        if best is None or dist_m < best[0]:
            best = (dist_m, slon, slat)

    if best is None:
        return lon, lat, []

    _, anchor_lon, anchor_lat = best
    if abs(lon - anchor_lon) < 1e-5 and abs(lat - anchor_lat) < 1e-5:
        return lon, lat, []

    prefix = [{"lon": float(anchor_lon), "lat": float(anchor_lat)}]
    return float(anchor_lon), float(anchor_lat), prefix


_CHANNEL_LANE_NODES: List[Tuple[float, float]] | None = None


def _channel_lane_nodes() -> List[Tuple[float, float]]:
    """Extra routing nodes on fairway channel boxes (computed once)."""
    global _CHANNEL_LANE_NODES
    if _CHANNEL_LANE_NODES is not None:
        return _CHANNEL_LANE_NODES
    nodes: List[Tuple[float, float]] = []
    for lon_min, lon_max, lat_min, lat_max in CHANNEL_BOXES:
        lon = lon_min
        while lon <= lon_max + 1e-9:
            lat = lat_min
            while lat <= lat_max + 1e-9:
                if is_baltic_water(lon, lat):
                    nodes.append((round(lon, 3), round(lat, 3)))
                lat += 0.55
            lon += 0.55
    _CHANNEL_LANE_NODES = nodes
    return _CHANNEL_LANE_NODES


def _stepped_water_path(
    start_lon: float,
    start_lat: float,
    goal_lon: float,
    goal_lat: float,
) -> List[Tuple[float, float]]:
    """Goal-directed walk with short validated legs (fallback when seed graph fails)."""
    cur_lon, cur_lat = float(start_lon), float(start_lat)
    path: List[Tuple[float, float]] = [(cur_lon, cur_lat)]
    for _ in range(_MAX_STEP_ROUTE_STEPS):
        if leg_samples_water(cur_lon, cur_lat, goal_lon, goal_lat):
            path.append((goal_lon, goal_lat))
            return path
        az12, _, dist_m = _GEOD.inv(cur_lon, cur_lat, goal_lon, goal_lat)
        dist_m = abs(dist_m)
        step_m = min(_STEP_ROUTE_M, dist_m)
        best: Tuple[float, float, float] | None = None
        candidates: List[Tuple[float, float]] = []
        nlon, nlat, _ = _GEOD.fwd(cur_lon, cur_lat, az12, step_m)
        candidates.append((nlon, nlat))
        candidates.append(snap_to_baltic_water(nlon, nlat))
        for bearing in range(0, 360, 20):
            lon2, lat2, _ = _GEOD.fwd(cur_lon, cur_lat, float(bearing), step_m * 0.75)
            candidates.append(snap_to_baltic_water(lon2, lat2))
        seen: set[Tuple[float, float]] = set()
        for nlon, nlat in candidates:
            key = (round(nlon, 3), round(nlat, 3))
            if key in seen:
                continue
            seen.add(key)
            if not is_baltic_water(nlon, nlat):
                continue
            if not leg_samples_water(cur_lon, cur_lat, nlon, nlat):
                continue
            _, _, rem = _GEOD.inv(nlon, nlat, goal_lon, goal_lat)
            rem = abs(rem)
            if best is None or rem < best[0]:
                best = (rem, nlon, nlat)
        if best is None:
            return []
        _, cur_lon, cur_lat = best
        path.append((cur_lon, cur_lat))
    return []


_STATIC_GRAPH_NODES: List[Tuple[float, float]] | None = None
_STATIC_GRAPH_ADJ: List[List[Tuple[int, float]]] | None = None


def _ensure_static_routing_graph() -> None:
    global _STATIC_GRAPH_NODES, _STATIC_GRAPH_ADJ
    if _STATIC_GRAPH_NODES is not None:
        return
    seen: set[Tuple[float, float]] = set()
    nodes: List[Tuple[float, float]] = []
    for lon, lat in _routing_seed_list() + _channel_lane_nodes():
        key = (round(lon, 3), round(lat, 3))
        if key in seen:
            continue
        seen.add(key)
        nodes.append(key)
    n = len(nodes)
    adj: List[List[Tuple[int, float]]] = [[] for _ in range(n)]
    for i in range(n):
        lon1, lat1 = nodes[i]
        for j in range(i + 1, n):
            lon2, lat2 = nodes[j]
            _, _, dist_m = _GEOD.inv(lon1, lat1, lon2, lat2)
            if abs(dist_m) > _STATIC_GRAPH_MAX_EDGE_M:
                continue
            if leg_samples_water(lon1, lat1, lon2, lat2):
                d = abs(dist_m)
                adj[i].append((j, d))
                adj[j].append((i, d))
    _STATIC_GRAPH_NODES = nodes
    _STATIC_GRAPH_ADJ = adj


def _dijkstra_path(
    nodes: List[Tuple[float, float]],
    adj: List[List[Tuple[int, float]]],
    start_i: int,
    goal_i: int,
) -> List[Tuple[float, float]]:
    n = len(nodes)
    dist = [1e18] * n
    dist[start_i] = 0.0
    prev = [-1] * n
    heap: List[Tuple[float, int]] = [(0.0, start_i)]
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


def _path_with_static_graph(
    start_lon: float,
    start_lat: float,
    goal_lon: float,
    goal_lat: float,
) -> List[Tuple[float, float]]:
    _ensure_static_routing_graph()
    assert _STATIC_GRAPH_NODES is not None
    assert _STATIC_GRAPH_ADJ is not None
    base = _STATIC_GRAPH_NODES
    base_adj = _STATIC_GRAPH_ADJ
    nodes: List[Tuple[float, float]] = [
        (start_lon, start_lat),
        *base,
        (goal_lon, goal_lat),
    ]
    n = len(nodes)
    start_i = 0
    goal_i = n - 1
    adj: List[List[Tuple[int, float]]] = [[] for _ in range(n)]
    for i in range(len(base)):
        for j, w in base_adj[i]:
            u, v = i + 1, j + 1
            adj[u].append((v, w))
            adj[v].append((u, w))
    for idx in (start_i, goal_i):
        lon_a, lat_a = nodes[idx]
        near: List[Tuple[float, int]] = []
        for j in range(1, n - 1):
            lon_b, lat_b = nodes[j]
            _, _, dist_m = _GEOD.inv(lon_a, lat_a, lon_b, lat_b)
            dist_m = abs(dist_m)
            if dist_m > _MAX_SEED_HOP_M:
                continue
            near.append((dist_m, j))
        near.sort(key=lambda x: x[0])
        attached = 0
        for _, j in near:
            if attached >= 18:
                break
            lon_b, lat_b = nodes[j]
            if not leg_samples_water(lon_a, lat_a, lon_b, lat_b):
                continue
            w = abs(_GEOD.inv(lon_a, lat_a, lon_b, lat_b)[2])
            adj[idx].append((j, w))
            adj[j].append((idx, w))
            attached += 1
    return _dijkstra_path(nodes, adj, start_i, goal_i)


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


def _destination_key(dest: Dict[str, Any]) -> Tuple[float, float]:
    return (round(float(dest["Longitude"]), 3), round(float(dest["Latitude"]), 3))


def _route_cache_key(
    start_lon: float,
    start_lat: float,
    harbor_lon: float,
    harbor_lat: float,
) -> Tuple[float, float, float, float]:
    return (
        round(start_lon, 2),
        round(start_lat, 2),
        round(harbor_lon, 2),
        round(harbor_lat, 2),
    )


def _cache_get_route(key: Tuple[float, float, float, float]) -> List[Dict[str, float]] | None:
    hit = _ROUTE_CACHE.get(key)
    if hit is None:
        return None
    return [dict(wp) for wp in hit]


def _cache_put_route(key: Tuple[float, float, float, float], waypoints: List[Dict[str, float]]) -> None:
    if len(_ROUTE_CACHE) >= _ROUTE_CACHE_MAX:
        _ROUTE_CACHE.pop(next(iter(_ROUTE_CACHE)))
    _ROUTE_CACHE[key] = [dict(wp) for wp in waypoints]


def _route_waypoints_valid(waypoints: List[Dict[str, float]]) -> bool:
    if not waypoints:
        return False
    for wp in waypoints:
        if not is_baltic_water(float(wp["lon"]), float(wp["lat"])):
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


def _bfs_water_path(
    start_lon: float,
    start_lat: float,
    goal_lon: float,
    goal_lat: float,
    max_hop_m: float | None = None,
) -> List[Tuple[float, float]]:
    del max_hop_m
    return _path_with_static_graph(start_lon, start_lat, goal_lon, goal_lat)


def _harbor_approach_points(harbor_lon: float, harbor_lat: float) -> List[Tuple[float, float]]:
    """Several offshore goal points near a harbor (routing retries)."""
    points: List[Tuple[float, float]] = []
    for seed in (0, 11, 23, 42, 57, 89)[:_HARBOR_APPROACH_SAMPLES]:
        lon, lat = sample_water_near_harbor(
            harbor_lon, harbor_lat, min_km=6.0, max_km=24.0, seed=seed
        )
        points.append((lon, lat))
    return points


def _water_graph_path(
    start_lon: float,
    start_lat: float,
    goal_lon: float,
    goal_lat: float,
) -> List[Tuple[float, float]]:
    """Shortest water-only path via precomputed fairway graph."""
    return _path_with_static_graph(start_lon, start_lat, goal_lon, goal_lat)


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
    route_lon, route_lat, prefix = _anchor_routing_start(start_lon, start_lat)

    cache_key = _route_cache_key(start_lon, start_lat, harbor_lon, harbor_lat)
    cached = _cache_get_route(cache_key)
    if cached is not None:
        return cached

    sparse: List[Dict[str, float]] = []
    for end_lon_try, end_lat_try in _harbor_approach_points(harbor_lon, harbor_lat):
        for path_fn in (_water_graph_path, _bfs_water_path):
            path = path_fn(route_lon, route_lat, end_lon_try, end_lat_try)
            if not path:
                continue
            tail = [{"lon": lon, "lat": lat} for lon, lat in path[1:]]
            candidate = prefix + tail
            if route_path_valid(start_lon, start_lat, candidate):
                sparse = candidate
                break
        if sparse:
            break

    if not sparse:
        for end_lon_try, end_lat_try in _harbor_approach_points(harbor_lon, harbor_lat):
            stepped = _stepped_water_path(route_lon, route_lat, end_lon_try, end_lat_try)
            if not stepped:
                continue
            tail = [{"lon": lon, "lat": lat} for lon, lat in stepped[1:]]
            candidate = prefix + tail
            if route_path_valid(start_lon, start_lat, candidate):
                sparse = candidate
                break

    _cache_put_route(cache_key, sparse)
    # Never return chord-snap fallbacks — they draw straight lines over peninsulas on maps.
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


def ensure_ship_route(ship: Dict[str, Any], *, force_rebuild: bool = False) -> None:
    """Attach ``RouteWaypoints`` / ``RouteIndex`` if missing or invalid."""
    dest = ship.get("Destination")
    if not isinstance(dest, dict):
        return
    start_lon = float(ship["Longitude"])
    start_lat = float(ship["Latitude"])
    dest_key = _destination_key(dest)
    _trim_passed_waypoints(ship)
    wps: List[Dict[str, float]] = ship.get("RouteWaypoints") or []
    idx = int(ship.get("RouteIndex", 0))

    if not force_rebuild and wps and idx < len(wps):
        if ship.get("RouteDestKey") == dest_key:
            return
        if ship.get("RouteDestKey") is None:
            ship["RouteDestKey"] = dest_key
            return

    remaining = wps[idx:] if idx < len(wps) else []
    if not force_rebuild and remaining and route_path_valid(start_lon, start_lat, remaining):
        if idx > 0:
            ship["RouteWaypoints"] = remaining
            ship["RouteIndex"] = 0
        ship["RouteDestKey"] = dest_key
        return

    ship["RouteWaypoints"] = build_sea_route(
        start_lon,
        start_lat,
        float(dest["Longitude"]),
        float(dest["Latitude"]),
    )
    ship["RouteIndex"] = 0
    ship["RouteDestKey"] = dest_key


def route_target(ship: Dict[str, Any]) -> Tuple[float, float] | None:
    """Current (lat, lon) waypoint, or None if route is empty."""
    wps: List[Dict[str, float]] = ship.get("RouteWaypoints") or []
    idx = int(ship.get("RouteIndex", 0))
    if idx >= len(wps):
        return None
    wp = wps[idx]
    return float(wp["lat"]), float(wp["lon"])


def at_waypoint(lat: float, lon: float, target_lat: float, target_lon: float) -> bool:
    _, _, dist_m = _GEOD.inv(lon, lat, target_lon, target_lat)
    return abs(dist_m) <= WAYPOINT_ARRIVAL_M


def pick_initial_destination_with_route(
    start_lon: float,
    start_lat: float,
    harbors: List[Dict[str, Any]],
    origin_name: str,
    *,
    max_attempts: int = 16,
) -> Tuple[Dict[str, Any], List[Dict[str, float]]]:
    """Choose a destination harbor and non-empty sea route when possible."""
    candidates = [h for h in harbors if h.get("name") != origin_name]
    random.shuffle(candidates)
    tries = candidates if len(candidates) <= max_attempts else candidates[:max_attempts]
    tries = tries + [c for c in candidates if c not in tries]
    for dest in tries[:max_attempts]:
        route = build_sea_route(
            start_lon,
            start_lat,
            float(dest["Longitude"]),
            float(dest["Latitude"]),
        )
        if route:
            return dest, route
    dest = random.choice(candidates)
    route = build_sea_route(
        start_lon,
        start_lat,
        float(dest["Longitude"]),
        float(dest["Latitude"]),
    )
    return dest, route


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
    ship["RouteDestKey"] = _destination_key(harbor)
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
    wps: List[Dict[str, float]] = ship.get("RouteWaypoints") or []
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
