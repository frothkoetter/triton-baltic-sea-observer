"""Tests for baltic_ship_sim_state route reset helpers."""

import json
import os
import tempfile
import unittest
from unittest.mock import patch

from baltic_ship_sim_state import (
    AIS_SHIP_STATE_FILE,
    reset_baltic_ship_route_state,
)
from marine_fleet_registry import MARINESHIP_STATE_FILE


class TestBalticShipSimState(unittest.TestCase):
    def test_delete_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            ais = os.path.join(tmp, "ship_state.json")
            marine = os.path.join(tmp, "marineship_state.json")
            with open(ais, "w") as f:
                json.dump([{"MMSI": "1", "RouteWaypoints": [{"lat": 1, "lon": 2}]}], f)
            with open(marine, "w") as f:
                json.dump([{"MMSI": "2"}], f)
            with patch("baltic_ship_sim_state.AIS_SHIP_STATE_FILE", ais), patch(
                "baltic_ship_sim_state.MARINESHIP_STATE_FILE", marine
            ):
                result = reset_baltic_ship_route_state(
                    reset_ais=True, reset_marine=True, mode="delete_files"
                )
            self.assertEqual(result.deleted_files, [ais, marine])
            self.assertFalse(os.path.exists(ais))
            self.assertFalse(os.path.exists(marine))

    def test_delete_files_captures_snapshot_before_remove(self):
        with tempfile.TemporaryDirectory() as tmp:
            ais = os.path.join(tmp, "ship_state.json")
            ships = [{"MMSI": "1", "RouteWaypoints": []}]
            with open(ais, "w") as f:
                json.dump(ships, f)
            with patch("baltic_ship_sim_state.AIS_SHIP_STATE_FILE", ais):
                result = reset_baltic_ship_route_state(
                    reset_ais=True, reset_marine=False, mode="delete_files"
                )
            self.assertEqual(result.snapshots[ais], ships)
            self.assertFalse(os.path.exists(ais))

    def test_clear_route_fields_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            ais = os.path.join(tmp, "ship_state.json")
            ships = [
                {
                    "MMSI": "1",
                    "Latitude": 54.0,
                    "Longitude": 12.0,
                    "RouteWaypoints": [{"Latitude": 55.0, "Longitude": 13.0}],
                    "RouteIndex": 2,
                }
            ]
            with open(ais, "w") as f:
                json.dump(ships, f)
            with patch("baltic_ship_sim_state.AIS_SHIP_STATE_FILE", ais):
                result = reset_baltic_ship_route_state(
                    reset_ais=True, reset_marine=False, mode="clear_route_fields"
                )
            self.assertIn(ais, result.updated_files)
            with open(ais) as f:
                loaded = json.load(f)
            self.assertEqual(loaded[0]["RouteWaypoints"], [])
            self.assertEqual(loaded[0]["RouteIndex"], 0)
            self.assertEqual(loaded[0]["Latitude"], 54.0)


if __name__ == "__main__":
    unittest.main()
