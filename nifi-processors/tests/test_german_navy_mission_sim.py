"""Tests for German Navy mission simulation engine."""

from __future__ import annotations

import random
import sys
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_UPLOAD = _ROOT / "upload"
for _p in (_UPLOAD.as_posix(), _ROOT.as_posix()):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from german_navy_mission_engine import (  # noqa: E402
    MISSION_ELIGIBLE_FRACTION,
    PORT_STAY_TICKS,
    count_mission_ships,
    home_base_for_unit,
    initialize_german_navy_ships,
    update_german_navy_movements,
    enrich_unit,
)
from marine_fleet_registry import fleet_unit_for_index  # noqa: E402

OPS = ["Fully Operational"]
SYS = ["All Systems Green"]


class TestGermanNavyMissionSim(unittest.TestCase):
    def test_mission_fraction_at_init(self):
        rng = random.Random(42)
        ships = initialize_german_navy_ships(20, rng=rng, operational_status_options=OPS, system_status_options=SYS)
        ratio = count_mission_ships(ships) / len(ships)
        self.assertGreaterEqual(ratio, 0.25)
        self.assertLessEqual(ratio, 0.45)
        self.assertAlmostEqual(MISSION_ELIGIBLE_FRACTION, 0.33, places=2)

    def test_home_bases_are_german_ports(self):
        u = enrich_unit(fleet_unit_for_index(0))
        base = home_base_for_unit(u)
        self.assertIn(base["name"], ("Kiel", "Wilhelmshaven", "Eckernförde", "Warnemünde"))

    def test_uboat_eckernfoerde(self):
        u = enrich_unit({"class": "U-Boote", "name": "U-31", "mmsi": "MAR123416"})
        self.assertEqual(home_base_for_unit(u)["name"], "Eckernförde")

    def test_port_stay_after_mission_ticks(self):
        self.assertGreater(PORT_STAY_TICKS, 100)

    def test_anchor_resume_patrol(self):
        rng = random.Random(99)
        ships = initialize_german_navy_ships(10, rng=rng, operational_status_options=OPS, system_status_options=SYS)
        patrol = next(s for s in ships if s.get("Phase") == "mission_patrol")
        patrol["Phase"] = "anchored"
        patrol["AnchorTicksRemaining"] = 1
        patrol["Speed"] = 0.0
        update_german_navy_movements(ships, rng=rng)
        self.assertEqual(patrol["Phase"], "mission_patrol")
        self.assertGreater(patrol["Speed"], 0.0)


if __name__ == "__main__":
    unittest.main()
