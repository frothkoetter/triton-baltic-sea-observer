#!/usr/bin/env python3
"""Stuck open-water positions with empty/invalid routes should recover."""

from __future__ import annotations

import os
import sys
import types
import unittest

UPLOAD = os.path.join(os.path.dirname(__file__), "..", "upload")
sys.path.insert(0, os.path.abspath(UPLOAD))

for mod in ("nifiapi", "nifiapi.flowfiletransform", "nifiapi.properties"):
    sys.modules.setdefault(mod, types.ModuleType(mod))
sys.modules["nifiapi.flowfiletransform"].FlowFileTransform = object
sys.modules["nifiapi.flowfiletransform"].FlowFileTransformResult = dict
sys.modules["nifiapi.properties"].PropertyDescriptor = lambda **k: k
sys.modules["nifiapi.properties"].StandardValidators = types.SimpleNamespace(
    POSITIVE_INTEGER_VALIDATOR=None,
)

from baltic_ship_routing import (  # noqa: E402
    recover_stuck_ship_route,
    route_path_valid,
    route_target,
    warm_routing_graph,
)
import ShipSimulationRoutesGemini as ais_sim  # noqa: E402


class TestStuckShipRecovery(unittest.TestCase):
    def setUp(self) -> None:
        warm_routing_graph()

    def test_open_water_empty_route_recovers_or_relocates(self) -> None:
        ship = {
            "MMSI": 123456002,
            "Longitude": 18.9,
            "Latitude": 54.5,
            "Speed": 8.0,
            "OriginPort": ais_sim.HARBOR_LOOKUP["Gdynia"],
            "Destination": ais_sim.HARBOR_LOOKUP["St. Petersburg"],
            "RouteWaypoints": [],
            "RouteIndex": 0,
        }
        self.assertTrue(recover_stuck_ship_route(ship, ais_sim.HARBORS))
        self.assertIsNotNone(route_target(ship))
        wps = ship.get("RouteWaypoints") or []
        self.assertTrue(wps)
        self.assertTrue(
            route_path_valid(
                float(ship["Longitude"]),
                float(ship["Latitude"]),
                wps[int(ship.get("RouteIndex", 0)) :],
            )
        )

    def test_invalid_overland_waypoints_cleared(self) -> None:
        ship = {
            "MMSI": 123456009,
            "Longitude": 13.05,
            "Latitude": 54.52,
            "OriginPort": ais_sim.HARBOR_LOOKUP["Stralsund"],
            "Destination": ais_sim.HARBOR_LOOKUP["Rostock"],
            "RouteWaypoints": [
                {"lon": 13.22, "lat": 54.52},
                {"lon": 12.14, "lat": 54.09},
            ],
            "RouteIndex": 0,
        }
        tail = ship["RouteWaypoints"]
        self.assertFalse(
            route_path_valid(
                float(ship["Longitude"]),
                float(ship["Latitude"]),
                tail,
            )
        )
        self.assertTrue(recover_stuck_ship_route(ship, ais_sim.HARBORS))
        wps = ship.get("RouteWaypoints") or []
        self.assertTrue(
            route_path_valid(
                float(ship["Longitude"]),
                float(ship["Latitude"]),
                wps,
            )
        )


if __name__ == "__main__":
    unittest.main()
