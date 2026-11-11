"""Tests for German fishing boat tour simulation."""

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

from fishing_boat_mission_engine import (  # noqa: E402
    FISHING_SPEED_MAX_KN,
    TOUR_MAX_TICKS,
    TOUR_MIN_TICKS,
    count_at_sea,
    home_port_for_unit,
    initialize_fishing_boats,
    update_fishing_boat_movements,
)
from fishing_fleet_registry import (  # noqa: E402
    GERMAN_FISHING_FLEET,
    GERMAN_FISHING_HARBORS,
    fleet_unit_for_index,
)


class TestFishingBoatMissionSim(unittest.TestCase):
    def test_fleet_size(self):
        self.assertEqual(len(GERMAN_FISHING_FLEET), 20)

    def test_home_ports_german_harbours(self):
        allowed = set(GERMAN_FISHING_HARBORS)
        for unit in GERMAN_FISHING_FLEET:
            self.assertIn(unit["home_port"], allowed)

    def test_tour_duration_bounds(self):
        rng = random.Random(7)
        ships = initialize_fishing_boats(10, rng=rng)
        for ship in ships:
            if ship.get("Phase") == "tour_fishing":
                rem = int(ship.get("TourTicksRemaining") or 0)
                self.assertGreaterEqual(rem, TOUR_MIN_TICKS)
                self.assertLessEqual(rem, TOUR_MAX_TICKS)

    def test_low_speed_at_sea(self):
        rng = random.Random(11)
        ships = initialize_fishing_boats(10, rng=rng)
        ships = update_fishing_boat_movements(ships, rng=rng)
        for ship in ships:
            if ship.get("Phase") in ("tour_fishing", "tour_return"):
                self.assertLessEqual(float(ship.get("Speed") or 0), FISHING_SPEED_MAX_KN + 0.5)

    def test_some_boats_at_sea_after_init(self):
        rng = random.Random(3)
        ships = initialize_fishing_boats(10, rng=rng)
        self.assertGreaterEqual(count_at_sea(ships), 1)

    def test_home_port_coords_match_registry(self):
        unit = fleet_unit_for_index(0)
        port = home_port_for_unit(unit)
        self.assertEqual(port["name"], unit["home_port"])


if __name__ == "__main__":
    unittest.main()
