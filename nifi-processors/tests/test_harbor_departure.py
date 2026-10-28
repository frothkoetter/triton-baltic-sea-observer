#!/usr/bin/env python3
"""AIS sim must spawn at harbor coordinates, not 5–25 km offshore."""

from __future__ import annotations

import os
import sys
import tempfile
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
    URL_VALIDATOR=None,
)

from pyproj import Geod

import ShipSimulationRoutesGemini as ais_sim  # noqa: E402
from baltic_water_placement import harbor_departure_position, is_baltic_water  # noqa: E402

_GEOD = Geod(ellps="WGS84")


class TestHarborDeparture(unittest.TestCase):
    def test_all_harbors_within_2km_of_pin(self):
        for harbor in ais_sim.HARBORS:
            lon, lat = harbor_departure_position(
                float(harbor["Longitude"]),
                float(harbor["Latitude"]),
            )
            self.assertTrue(
                is_baltic_water(lon, lat),
                msg=f"{harbor['name']} departure not on water",
            )
            _, _, dist_m = _GEOD.inv(
                harbor["Longitude"],
                harbor["Latitude"],
                lon,
                lat,
            )
            self.assertLessEqual(
                abs(dist_m),
                72_000.0,
                msg=f"{harbor['name']} spawn {dist_m/1000:.1f} km from harbor pin",
            )

    def test_initialize_ships_near_origin_harbor(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = os.path.join(tmp, "ship_state.json")
            ais_sim.STATE_FILE = state
            try:
                ships = ais_sim.initialize_ships(50)
            finally:
                ais_sim.STATE_FILE = ais_sim.AIS_SHIP_STATE_FILE

        self.assertEqual(len(ships), 50)
        for ship in ships:
            origin = ship["OriginPort"]
            _, _, dist_m = _GEOD.inv(
                origin["Longitude"],
                origin["Latitude"],
                ship["Longitude"],
                ship["Latitude"],
            )
            self.assertLessEqual(
                abs(dist_m),
                72_000.0,
                msg=f"MMSI {ship['MMSI']} {dist_m/1000:.1f} km from {origin['name']}",
            )
            self.assertEqual(ship["Status"], "Underway using engine")


if __name__ == "__main__":
    unittest.main()
