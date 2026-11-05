#!/usr/bin/env python3
"""Harbor-pair fallback table sanity checks."""

from __future__ import annotations

import unittest

from baltic_ship_routing import (
    DEFAULT_HARBOR_PAIR_FALLBACKS,
    HARBOR_PAIR_FALLBACKS,
    assign_routable_destination,
    warm_routing_graph,
)
from baltic_water_placement import sample_water_near_harbor


class TestHarborPairFallbacks(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        warm_routing_graph()

    def test_fallback_names_are_nonempty(self) -> None:
        self.assertGreater(len(DEFAULT_HARBOR_PAIR_FALLBACKS), 3)
        for origin, names in HARBOR_PAIR_FALLBACKS.items():
            self.assertGreater(len(names), 0, origin)
            self.assertNotIn(origin, names)

    def test_stuck_snapshot_origins_get_routes(self) -> None:
        """Origins that appeared in stuck-ship NDJSON analysis."""
        harbors = [
            {"name": "Stockholm", "Latitude": 59.3293, "Longitude": 18.0686},
            {"name": "Mariehamn", "Latitude": 60.0973, "Longitude": 19.9348},
            {"name": "Nynäshamn", "Latitude": 58.9036, "Longitude": 17.9470},
            {"name": "Sassnitz", "Latitude": 54.5183, "Longitude": 13.6414},
            {"name": "Turku", "Latitude": 60.4518, "Longitude": 22.2666},
            {"name": "Helsinki", "Latitude": 60.1695, "Longitude": 24.9354},
            {"name": "Tallinn", "Latitude": 59.4370, "Longitude": 24.7536},
            {"name": "Riga", "Latitude": 56.9496, "Longitude": 24.1052},
            {"name": "Rostock", "Latitude": 54.0887, "Longitude": 12.1405},
            {"name": "Karlskrona", "Latitude": 56.1612, "Longitude": 15.5869},
            {"name": "Travemünde", "Latitude": 53.9624, "Longitude": 10.8672},
            {"name": "Gdynia", "Latitude": 54.5189, "Longitude": 18.5305},
            {"name": "Klaipėda", "Latitude": 55.7033, "Longitude": 21.1443},
            {"name": "Visby", "Latitude": 57.6409, "Longitude": 18.2960},
            {"name": "Rønne", "Latitude": 55.1037, "Longitude": 14.7065},
        ]
        for origin_name in ("Stockholm", "Turku", "Mariehamn", "Sassnitz", "Rønne"):
            ha = next(h for h in harbors if h["name"] == origin_name)
            lon, lat = sample_water_near_harbor(
                ha["Longitude"], ha["Latitude"], 5.0, 22.0, seed=2
            )
            ship = {
                "Longitude": lon,
                "Latitude": lat,
                "OriginPort": ha,
                "Destination": {"name": "Travemünde", "Latitude": 53.9624, "Longitude": 10.8672},
            }
            ok = assign_routable_destination(ship, harbors, prefer_current=False)
            self.assertTrue(ok, origin_name)
            self.assertTrue(ship.get("RouteWaypoints"), origin_name)


if __name__ == "__main__":
    unittest.main()
