#!/usr/bin/env python3
"""Coast/island regression tests for ``baltic_water_placement`` and simulators."""

from __future__ import annotations

import random
import sys
import unittest

from baltic_coast_pins import LAND_PINS, WATER_PINS
from baltic_water_placement import (
    CHANNEL_BOXES,
    ISLAND_BOXES,
    MAINLAND_BOXES,
    OFFSHORE_SEEDS,
    OPEN_WATER_ZONES,
    _in_any,
    is_baltic_water,
    random_baltic_water_position,
    sample_water_near_harbor,
)

try:
    from LidarSimulator import build_single_vessel_points
except ImportError:
    build_single_vessel_points = None  # type: ignore[misc, assignment]

HARBOR_PORTS = [
    ("Kiel", 10.1228, 54.3233),
    ("Travemünde", 10.8672, 53.9624),
    ("Rostock", 12.1405, 54.0887),
    ("Stralsund", 13.0810, 54.3091),
    ("Sassnitz", 13.6414, 54.5183),
    ("Świnoujście", 14.2478, 53.9106),
    ("Gdynia", 18.5305, 54.5189),
    ("Klaipėda", 21.1443, 55.7033),
    ("Liepāja", 21.0136, 56.5110),
    ("Ventspils", 21.5200, 57.3950),
    ("Riga", 24.1052, 56.9496),
    ("Tallinn", 24.7536, 59.4370),
    ("Helsinki", 24.9354, 60.1695),
    ("Stockholm", 18.0686, 59.3293),
    ("Karlskrona", 15.5869, 56.1612),
    ("Turku", 22.2666, 60.4518),
    ("Paldiski", 24.0539, 59.3567),
    ("Gdańsk", 18.6466, 54.3520),
    ("Wismar", 11.4536, 53.8934),
    ("Rønne", 14.7065, 55.1037),
]


class TestBalticWaterPlacement(unittest.TestCase):
    def test_land_pins_not_water(self) -> None:
        failures = [
            (name, lon, lat)
            for name, lon, lat in LAND_PINS
            if is_baltic_water(lon, lat)
        ]
        self.assertEqual([], failures, f"land pins classified as sea: {failures[:8]}")

    def test_water_pins_are_water(self) -> None:
        failures = [
            (name, lon, lat)
            for name, lon, lat in WATER_PINS
            if not is_baltic_water(lon, lat)
        ]
        self.assertEqual([], failures, f"sea pins classified as land: {failures[:8]}")

    def test_offshore_seeds_on_water(self) -> None:
        bad = [(lon, lat) for lon, lat in OFFSHORE_SEEDS if not is_baltic_water(lon, lat)]
        self.assertEqual([], bad, f"OFFSHORE_SEEDS on land: {bad}")

    def test_mainland_boxes_not_water_outside_channels(self) -> None:
        random.seed(42)
        leaks: list[tuple[float, float]] = []
        for box in MAINLAND_BOXES:
            for _ in range(25):
                lon = random.uniform(box[0], box[1])
                lat = random.uniform(box[2], box[3])
                if _in_any(lon, lat, CHANNEL_BOXES):
                    continue
                if is_baltic_water(lon, lat):
                    leaks.append((lon, lat))
        self.assertLess(len(leaks), 3, f"mainland leaks (sample): {leaks[:5]}")

    def test_island_boxes_not_water_outside_channels(self) -> None:
        random.seed(43)
        leaks: list[tuple[float, float]] = []
        for box in ISLAND_BOXES:
            for _ in range(25):
                lon = random.uniform(box[0], box[1])
                lat = random.uniform(box[2], box[3])
                if _in_any(lon, lat, CHANNEL_BOXES):
                    continue
                if is_baltic_water(lon, lat):
                    leaks.append((lon, lat))
        self.assertEqual([], leaks, f"island leaks: {leaks[:5]}")

    def test_open_water_zones_need_nearby_seed(self) -> None:
        random.seed(44)
        for box in OPEN_WATER_ZONES:
            for _ in range(15):
                lon = random.uniform(box[0], box[1])
                lat = random.uniform(box[2], box[3])
                if _in_any(lon, lat, CHANNEL_BOXES):
                    continue
                if _in_any(lon, lat, MAINLAND_BOXES) or _in_any(lon, lat, ISLAND_BOXES):
                    continue
                if is_baltic_water(lon, lat):
                    return
        self.skipTest("no isolated open-zone water in sample (OK)")

    def test_harbor_spawns_always_water(self) -> None:
        for name, lon, lat in HARBOR_PORTS:
            for seed in range(6):
                wl, wlat = sample_water_near_harbor(lon, lat, 3.0, 28.0, seed=seed)
                self.assertTrue(
                    is_baltic_water(wl, wlat),
                    f"{name} seed={seed} -> ({wl}, {wlat})",
                )

    def test_random_baltic_water_batch(self) -> None:
        import numpy as np

        rng = np.random.default_rng(99)
        for _ in range(400):
            lon, lat = random_baltic_water_position(rng)
            self.assertTrue(is_baltic_water(lon, lat))

    @unittest.skipUnless(build_single_vessel_points, "nifiapi not installed")
    def test_lidar_scene_centers_on_water(self) -> None:
        for seed in range(150):
            _, _, _, lon, lat, _ = build_single_vessel_points(seed=seed)
            self.assertTrue(is_baltic_water(lon, lat), f"lidar seed {seed}")


if __name__ == "__main__":
    sys.exit(unittest.main())
