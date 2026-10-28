#!/usr/bin/env python3
"""Unit tests for Baltic harbor sea routing."""

from __future__ import annotations

import random
import unittest

from baltic_ship_routing import (
    build_sea_route,
    pick_initial_destination_with_route,
    route_path_valid,
)
from baltic_water_placement import harbor_departure_position, sample_water_near_harbor

HARBORS = [
    {"name": "Stockholm", "Latitude": 59.3293, "Longitude": 18.0686},
    {"name": "Helsinki", "Latitude": 60.1695, "Longitude": 24.9354},
    {"name": "Tallinn", "Latitude": 59.4370, "Longitude": 24.7536},
    {"name": "Riga", "Latitude": 56.9496, "Longitude": 24.1052},
    {"name": "Gdynia", "Latitude": 54.5189, "Longitude": 18.5305},
    {"name": "Klaipėda", "Latitude": 55.7033, "Longitude": 21.1443},
    {"name": "Liepāja", "Latitude": 56.5110, "Longitude": 21.0136},
    {"name": "Kiel", "Latitude": 54.3233, "Longitude": 10.1228},
    {"name": "Rostock", "Latitude": 54.0887, "Longitude": 12.1405},
    {"name": "Stralsund", "Latitude": 54.3091, "Longitude": 13.0810},
    {"name": "Karlskrona", "Latitude": 56.1612, "Longitude": 15.5869},
    {"name": "Ventspils", "Latitude": 57.395, "Longitude": 21.520},
    {"name": "St. Petersburg", "Latitude": 59.904, "Longitude": 29.773},
]


class TestBalticShipRouting(unittest.TestCase):
    def test_known_harbor_pairs_route(self) -> None:
        pairs = [
            ("Ventspils", "St. Petersburg"),
            ("Kiel", "Rostock"),
            ("Riga", "Helsinki"),
            ("Rostock", "Stralsund"),
            ("Liepāja", "Tallinn"),
        ]
        for a_name, b_name in pairs:
            ha = next(h for h in HARBORS if h["name"] == a_name)
            hb = next(h for h in HARBORS if h["name"] == b_name)
            lon, lat = sample_water_near_harbor(
                ha["Longitude"], ha["Latitude"], 5.0, 22.0, seed=3
            )
            route = build_sea_route(lon, lat, hb["Longitude"], hb["Latitude"])
            self.assertTrue(route, f"empty route {a_name} -> {b_name}")
            self.assertTrue(
                route_path_valid(lon, lat, route),
                f"invalid route {a_name} -> {b_name}",
            )

    def test_fleet_init_mostly_routed(self) -> None:
        random.seed(7)
        ok = 0
        for _ in range(50):
            origin = random.choice(HARBORS)
            lon, lat = harbor_departure_position(
                float(origin["Longitude"]),
                float(origin["Latitude"]),
            )
            _dest, route = pick_initial_destination_with_route(
                lon, lat, HARBORS, origin["name"]
            )
            if route:
                ok += 1
        self.assertGreaterEqual(ok, 45, f"only {ok}/50 ships got routes")


if __name__ == "__main__":
    unittest.main()
