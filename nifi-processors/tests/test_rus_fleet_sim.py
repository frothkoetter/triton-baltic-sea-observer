"""Unit tests for Russian Baltic Fleet simulation engine."""

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

from rus_fleet_registry import (  # noqa: E402
    BALTIYSK_HOME_FRACTION,
    MISSION_ELIGIBLE_FRACTION,
    RUS_BALTIC_FLEET,
)
from rus_fleet_sim_engine import (  # noqa: E402
    ANCHOR_MAX_TICKS,
    MISSION_MAX_TICKS,
    MISSION_MIN_TICKS,
    PORT_STAY_TICKS,
    count_mission_ships,
    home_base_distribution,
    initialize_rus_fleet_ships,
    update_rus_fleet_movements,
)


class TestRusFleetSim(unittest.TestCase):
    def test_mission_fraction_at_init(self):
        rng = random.Random(42)
        ships = initialize_rus_fleet_ships(len(RUS_BALTIC_FLEET), rng=rng)
        active = count_mission_ships(ships)
        ratio = active / len(ships)
        self.assertGreaterEqual(ratio, 0.28)
        self.assertLessEqual(ratio, 0.40)
        self.assertAlmostEqual(MISSION_ELIGIBLE_FRACTION, 0.33, places=2)

    def test_home_base_distribution_standby(self):
        rng = random.Random(7)
        ships = initialize_rus_fleet_ships(len(RUS_BALTIC_FLEET), rng=rng)
        standby = [s for s in ships if s.get("Phase") == "home_port"]
        dist = home_base_distribution(ships)
        baltiysk = dist.get("Baltiysk", 0)
        if standby:
            share = baltiysk / len(standby)
            self.assertGreater(share, 0.45)
            self.assertLess(share, 0.85)
        self.assertAlmostEqual(BALTIYSK_HOME_FRACTION, 0.60, places=1)

    def test_anchor_state_transition(self):
        rng = random.Random(99)
        ships = initialize_rus_fleet_ships(10, rng=rng)
        ship = next(s for s in ships if s.get("Phase") == "mission_patrol")
        ship["AnchorTicksRemaining"] = 2
        ship["Phase"] = "anchored"
        ship["Speed"] = 0.0
        ship["Status"] = "Anchored"
        update_rus_fleet_movements(ships, rng=rng)
        self.assertEqual(ship["AnchorTicksRemaining"], 1)
        self.assertEqual(ship["Status"], "Anchored")
        update_rus_fleet_movements(ships, rng=rng)
        self.assertEqual(ship["Phase"], "mission_patrol")
        self.assertGreater(ship["Speed"], 0.0)

    def test_mission_duration_bounds_constants(self):
        self.assertLess(MISSION_MIN_TICKS, MISSION_MAX_TICKS)
        self.assertGreater(MISSION_MIN_TICKS, 0)
        self.assertGreater(PORT_STAY_TICKS, 100)
        self.assertLessEqual(MISSION_MAX_TICKS, MISSION_MIN_TICKS * 3)
        self.assertGreaterEqual(ANCHOR_MAX_TICKS, 1)

    def test_mission_ticks_decrement_on_patrol(self):
        rng = random.Random(123)
        ships = initialize_rus_fleet_ships(15, rng=rng)
        patrol = [s for s in ships if s.get("Phase") == "mission_patrol"]
        self.assertTrue(patrol)
        before = int(patrol[0]["MissionTicksRemaining"])
        update_rus_fleet_movements(ships, rng=rng)
        after = int(patrol[0]["MissionTicksRemaining"])
        self.assertEqual(after, before - 1)
        self.assertGreaterEqual(before, MISSION_MIN_TICKS)
        self.assertLessEqual(before, MISSION_MAX_TICKS)


if __name__ == "__main__":
    unittest.main()
