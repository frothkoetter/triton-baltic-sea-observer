"""Fishing boat LAS hull must not share class 10 with sea surface."""

import sys
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "upload"))

from LidarSimulator import build_single_vessel_points  # noqa: E402
from lidar_sea_surface import LAS_CLASS_WATER_SURFACE  # noqa: E402
from lidar_vessel_geometry import VESSEL_TYPE_CODES  # noqa: E402


class TestLidarFishingBoatClass(unittest.TestCase):
    def test_fishing_hull_class_distinct_from_water(self):
        self.assertEqual(VESSEL_TYPE_CODES["fishing_boat"], 11)
        self.assertEqual(LAS_CLASS_WATER_SURFACE, 10)

    def test_forced_fishing_boat_emits_class_11_on_hull(self):
        pts, classification, spec, _, _, uncl = build_single_vessel_points(
            seed=99,
            unclassified_fraction=0.0,
            sea_surface_budget=200,
            target_vessel_type="fishing_boat",
        )
        self.assertEqual(spec.vessel_type, "fishing_boat")
        self.assertFalse(uncl)
        hull_codes = set(classification[classification != LAS_CLASS_WATER_SURFACE])
        self.assertEqual(hull_codes, {11})
        self.assertGreater(int((classification == 11).sum()), 100)
        self.assertGreater(int((classification == LAS_CLASS_WATER_SURFACE).sum()), 0)


if __name__ == "__main__":
    unittest.main()
