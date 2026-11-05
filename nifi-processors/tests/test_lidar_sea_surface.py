#!/usr/bin/env python3
"""Sparse sea surface points around LiDAR vessel clouds."""

from __future__ import annotations

import os
import sys
import unittest

UPLOAD = os.path.join(os.path.dirname(__file__), "..", "upload")
sys.path.insert(0, os.path.abspath(UPLOAD))

from lidar_sea_surface import LAS_CLASS_WATER_SURFACE, SeaSurfaceConfig, sample_sparse_sea_surface_local
from lidar_vessel_geometry import VesselSpec, baltic_fleet_scene


class TestLidarSeaSurface(unittest.TestCase):
    def test_budget_cap_and_hole_around_hull(self) -> None:
        spec = baltic_fleet_scene()[0]
        rng = __import__("numpy").random.default_rng(7)
        pts = sample_sparse_sea_surface_local(
            spec,
            rng,
            SeaSurfaceConfig(point_budget=800, wave_amplitude_m=0.5),
        )
        self.assertGreater(len(pts), 100)
        self.assertLessEqual(len(pts), 800)
        rx = spec.length_m / 2 + 4.0
        ry = spec.beam_m / 2 + 4.0
        inside = (pts[:, 0] / rx) ** 2 + (pts[:, 1] / ry) ** 2 < 1.0
        self.assertLess(int(inside.sum()), len(pts) * 0.05)

    def test_zero_budget_empty(self) -> None:
        spec = baltic_fleet_scene()[3]
        rng = __import__("numpy").random.default_rng(1)
        pts = sample_sparse_sea_surface_local(
            spec, rng, SeaSurfaceConfig(point_budget=0)
        )
        self.assertEqual(len(pts), 0)

    def test_water_class_constant(self) -> None:
        self.assertEqual(LAS_CLASS_WATER_SURFACE, 10)


if __name__ == "__main__":
    unittest.main()
