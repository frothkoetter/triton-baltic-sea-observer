#!/usr/bin/env python3
"""Quick check: harbor routes and simulated ticks stay on baltic_water."""

from __future__ import annotations

import random
import sys

from baltic_ship_routing import build_sea_route, ensure_ship_route, route_target, advance_route_or_arrive
from baltic_water_placement import is_baltic_water, sample_water_near_harbor

# Subset of ShipSimulationRoutesGemini harbors (no nifiapi import).
HARBORS = [
    {"name": "Ventspils", "Latitude": 57.3890, "Longitude": 21.5610},
    {"name": "Primorsk", "Latitude": 60.338, "Longitude": 28.480},
    {"name": "St. Petersburg", "Latitude": 59.904, "Longitude": 29.773},
    {"name": "Liepāja", "Latitude": 56.5110, "Longitude": 21.0136},
    {"name": "Tallinn", "Latitude": 59.4370, "Longitude": 24.7536},
    {"name": "Riga", "Latitude": 56.9496, "Longitude": 24.1052},
    {"name": "Helsinki", "Latitude": 60.1695, "Longitude": 24.9354},
]

NM_TO_DEG = 0.016666666666666666


def _move_towards(lat1, lon1, lat2, lon2, speed):
    dlat, dlon = lat2 - lat1, lon2 - lon1
    dist = (dlat**2 + dlon**2) ** 0.5
    if dist == 0:
        return lat2, lon2
    r = min(1.0, speed * NM_TO_DEG / dist)
    return lat1 + dlat * r, lon1 + dlon * r


def _move_on_water(lat1, lon1, lat2, lon2, speed):
    from baltic_water_placement import snap_to_baltic_water

    lat, lon = lat1, lon1
    for _ in range(5):
        lat, lon = _move_towards(lat, lon, lat2, lon2, speed / 5)
        if not is_baltic_water(lon, lat):
            lon, lat = snap_to_baltic_water(lon, lat)
    return lat, lon


def _simulate(ha, hb, ticks=600):
    lon, lat = sample_water_near_harbor(ha["Longitude"], ha["Latitude"], 5, 20, seed=1)
    ship = {
        "Longitude": lon,
        "Latitude": lat,
        "Speed": 8.0,
        "Destination": hb,
        "RouteWaypoints": [],
        "RouteIndex": 0,
    }
    ensure_ship_route(ship)
    land = 0
    for _ in range(ticks):
        tgt = route_target(ship)
        if not tgt:
            break
        tlat, tlon = tgt
        lat, lon = _move_on_water(ship["Latitude"], ship["Longitude"], tlat, tlon, ship["Speed"])
        if not is_baltic_water(lon, lat):
            land += 1
        ship["Latitude"], ship["Longitude"] = lat, lon
        advance_route_or_arrive(ship, HARBORS, lat, lon)
    return land


def main() -> int:
    failures = 0
    for a, b in [
        ("Ventspils", "Primorsk"),
        ("Ventspils", "St. Petersburg"),
        ("Liepāja", "Tallinn"),
        ("Riga", "Helsinki"),
    ]:
        ha = next(h for h in HARBORS if h["name"] == a)
        hb = next(h for h in HARBORS if h["name"] == b)
        route = build_sea_route(ha["Longitude"], ha["Latitude"], hb["Longitude"], hb["Latitude"])
        if not route or not all(is_baltic_water(w["lon"], w["lat"]) for w in route):
            print(f"FAIL waypoints {a} -> {b}")
            failures += 1
            continue
        land = _simulate(ha, hb)
        if land:
            print(f"FAIL sim land ticks {a} -> {b}: {land}")
            failures += 1
        else:
            print(f"OK {a} -> {b} ({len(route)} waypoints)")

    if is_baltic_water(28.33, 57.81):
        print("FAIL Pskov should be land")
        failures += 1

    random.seed(0)
    land_sum = sum(_simulate(*random.sample(HARBORS, 2), ticks=300) for _ in range(20))
    if land_sum:
        print(f"FAIL random pairs land ticks={land_sum}")
        failures += 1
    else:
        print("OK 20 random harbor pair simulations")

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
