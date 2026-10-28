#!/usr/bin/env python3
"""Quick check: harbor routes and simulated ticks stay on baltic_water."""

from __future__ import annotations

import random
import sys

from pyproj import Geod

from baltic_ship_routing import (
    build_sea_route,
    ensure_ship_route,
    move_towards_on_water,
    route_path_valid,
    route_target,
    advance_route_or_arrive,
)
from baltic_water_placement import is_baltic_water, sample_water_near_harbor

_GEOD = Geod(ellps="WGS84")

# Subset of ShipSimulationRoutesGemini harbors (no nifiapi import).
HARBORS = [
    {"name": "Stralsund", "Latitude": 54.3091, "Longitude": 13.0810},
    {"name": "Sassnitz", "Latitude": 54.5183, "Longitude": 13.6414},
    {"name": "Rostock", "Latitude": 54.0887, "Longitude": 12.1405},
    {"name": "Ventspils", "Latitude": 57.395, "Longitude": 21.520},
    {"name": "Primorsk", "Latitude": 60.338, "Longitude": 28.480},
    {"name": "St. Petersburg", "Latitude": 59.904, "Longitude": 29.773},
    {"name": "Liepāja", "Latitude": 56.5110, "Longitude": 21.0136},
    {"name": "Tallinn", "Latitude": 59.4370, "Longitude": 24.7536},
    {"name": "Riga", "Latitude": 56.9496, "Longitude": 24.1052},
    {"name": "Helsinki", "Latitude": 60.1695, "Longitude": 24.9354},
]


def _simulate(ha, hb, ticks=180):
    lon, lat = sample_water_near_harbor(ha["Longitude"], ha["Latitude"], 5, 20, seed=1)
    _, _, dist_km = _GEOD.inv(ha["Longitude"], ha["Latitude"], lon, lat)
    if abs(dist_km) > 35_000:
        print(f"WARN spawn far from {ha['name']}: {abs(dist_km)/1000:.0f} km")
    ship = {
        "Longitude": lon,
        "Latitude": lat,
        "Speed": 8.0,
        "OriginPort": ha,
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
        lat, lon = move_towards_on_water(ship["Latitude"], ship["Longitude"], tlat, tlon, ship["Speed"])
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
        ("Ventspils", "Helsinki"),
        ("Liepāja", "Tallinn"),
        ("Riga", "Helsinki"),
        ("Stralsund", "Sassnitz"),
        ("Rostock", "Stralsund"),
    ]:
        ha = next(h for h in HARBORS if h["name"] == a)
        hb = next(h for h in HARBORS if h["name"] == b)
        lon, lat = sample_water_near_harbor(ha["Longitude"], ha["Latitude"], 5, 20, seed=1)
        route = build_sea_route(lon, lat, hb["Longitude"], hb["Latitude"])
        if not route or not route_path_valid(lon, lat, route):
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

    for seed in range(10):
        lon, lat = sample_water_near_harbor(21.520, 57.395, 5, 25, seed=seed)
        _, _, d = _GEOD.inv(21.520, 57.395, lon, lat)
        if abs(d) > 30_000:
            print(f"FAIL Ventspils spawn seed={seed} dist={abs(d)/1000:.0f} km")
            failures += 1

    random.seed(0)
    land_sum = sum(_simulate(*random.sample(HARBORS, 2), ticks=120) for _ in range(12))
    if land_sum:
        print(f"FAIL random pairs land ticks={land_sum}")
        failures += 1
    else:
        print("OK random harbor pair simulations")

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
