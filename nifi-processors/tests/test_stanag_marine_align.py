"""STANAG POSITION alignment with MarineShipSimulator."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_UPLOAD = _ROOT / "upload"
for _p in (_UPLOAD.as_posix(), _ROOT.as_posix()):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from marine_fleet_registry import (  # noqa: E402
    MARINE_POSITION_DECIMALS,
    MARINESHIP_STATE_FILE,
    load_simulated_ship_positions,
    parse_marine_simulator_ndjson,
)
from german_navy_mission_engine import initialize_german_navy_ships  # noqa: E402


class TestStanagMarineAlign(unittest.TestCase):
    def test_parse_marine_ndjson_matches_ais_rounding(self):
        ndjson = "\n".join(
            [
                json.dumps(
                    {
                        "MMSI": "MAR123422",
                        "message_from": "FGS Fulda",
                        "Latitude": 54.19012,
                        "Longitude": 12.03574,
                        "Speed": 8.0,
                    }
                ),
                json.dumps(
                    {
                        "MMSI": "MAR123424",
                        "message_from": "FGS Sulzbach-Rosenberg",
                        "Latitude": 54.09111,
                        "Longitude": 12.14002,
                    }
                ),
            ]
        )
        rows = parse_marine_simulator_ndjson(ndjson)
        self.assertEqual(len(rows), 2)
        fulda = next(r for r in rows if r["name"] == "FGS Fulda")
        self.assertEqual(fulda["lat"], round(54.19012, MARINE_POSITION_DECIMALS))
        self.assertEqual(fulda["lon"], round(12.03574, MARINE_POSITION_DECIMALS))

    def test_stanag_position_line_uses_same_coords(self):
        ndjson = json.dumps(
            {
                "MMSI": "MAR123422",
                "message_from": "FGS Fulda",
                "Latitude": 54.19012,
                "Longitude": 12.03574,
            }
        )
        ship = parse_marine_simulator_ndjson(ndjson)[0]
        lat = round(ship["lat"], MARINE_POSITION_DECIMALS)
        lon = round(ship["lon"], MARINE_POSITION_DECIMALS)
        pos = f"LAT {lat}°N, LON {lon}°E"
        self.assertRegex(pos, r"LAT 54\.19012°N, LON 12\.03574°E")

    def test_load_state_file_rounding(self):
        rng = __import__("random").Random(0)
        ships = initialize_german_navy_ships(
            3,
            rng=rng,
            operational_status_options=["Fully Operational"],
            system_status_options=["All Systems Green"],
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "marineship_state.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(ships, f)
            import marine_fleet_registry as mfr

            old = mfr.MARINESHIP_STATE_FILE
            try:
                mfr.MARINESHIP_STATE_FILE = path
                loaded = load_simulated_ship_positions()
            finally:
                mfr.MARINESHIP_STATE_FILE = old
        self.assertEqual(len(loaded), 3)
        for row in loaded:
            self.assertEqual(row["lat"], round(row["lat"], MARINE_POSITION_DECIMALS))
            self.assertIn("name", row)
            self.assertTrue(row["name"].startswith("FGS") or row["name"].startswith("U-"))


if __name__ == "__main__":
    unittest.main()