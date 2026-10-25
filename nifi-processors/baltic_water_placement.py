"""
Offshore placement for the Baltic LiDAR simulator (stdlib + pyproj only).

Open-sea positions are drawn from known water seeds / sea zones and rejected if they
fall on coarse mainland or island boxes. Port offsets retry until water or seaward.
"""

from __future__ import annotations

from typing import List, Sequence, Tuple

import numpy as np
from pyproj import Geod

_GEOD = Geod(ellps="WGS84")

# (lon_min, lon_max, lat_min, lat_max)
Box = Tuple[float, float, float, float]

# Curated offshore points in the Baltic proper (WGS84).
OFFSHORE_SEEDS: List[Tuple[float, float]] = [
    (11.2, 56.8),
    (11.8, 57.4),
    (12.6, 56.1),
    (13.8, 55.2),
    (14.8, 55.0),
    (16.0, 55.4),
    (17.2, 56.2),
    (18.5, 57.0),
    (19.8, 57.2),
    (21.2, 56.8),
    (22.5, 57.5),
    (24.0, 57.8),
    (25.5, 58.2),
    (26.8, 59.0),
    (25.0, 59.8),
    (23.5, 60.2),
    (21.0, 59.5),
    (19.0, 58.8),
    (17.5, 58.0),
    (16.2, 57.2),
    (15.0, 56.5),
    (20.5, 55.5),
    (22.0, 56.0),
    (24.5, 56.2),
    (27.5, 57.0),
    (28.5, 58.0),
    (13.2, 54.6),
    (15.5, 54.8),
    (18.0, 54.5),
    (20.0, 54.8),
]

# Uniform random fallback only inside these sea-heavy rectangles.
OPEN_WATER_ZONES: List[Box] = [
    (15.2, 17.2, 54.55, 55.35),
    (17.0, 20.5, 55.0, 56.8),
    (19.5, 23.0, 56.5, 58.2),
    (22.0, 27.0, 57.5, 59.5),
    (24.0, 28.5, 58.5, 60.15),
    (11.0, 13.5, 56.0, 57.8),
    (13.5, 15.5, 55.0, 56.2),
]

# Islands and archipelago cores (exclude; jitter from seeds may land here).
ISLAND_BOXES: List[Box] = [
    (14.70, 15.35, 54.95, 55.40),  # Bornholm
    (17.70, 19.40, 56.85, 57.95),  # Gotland
    (16.55, 17.25, 56.15, 56.55),  # Öland
    (19.40, 20.20, 57.80, 58.20),  # Åland (main)
    (22.80, 23.60, 58.70, 59.20),  # Hiiumaa
    (22.00, 23.10, 58.10, 58.65),  # Saaremaa
    (13.30, 13.75, 54.95, 55.20),  # Fehmarn
    (10.90, 11.40, 54.85, 55.10),  # Lolland/Falster gap
]

# Mainland / large coastal land (conservative; avoids Sweden, Finland, PL, DE, DK).
MAINLAND_BOXES: List[Box] = [
    (8.0, 14.5, 53.5, 55.45),
    (10.5, 16.8, 55.45, 58.85),
    (16.8, 18.05, 57.8, 60.6),
    (18.05, 21.5, 59.2, 60.6),
    (21.5, 28.8, 59.8, 60.6),
    (14.0, 19.5, 53.5, 54.55),
    (19.5, 21.8, 53.8, 54.55),
    (21.8, 24.5, 53.9, 55.8),
    (24.5, 28.5, 54.0, 57.5),
    (12.0, 14.8, 54.5, 55.55),
]

BALTIC_BOUNDS: Box = (10.0, 30.0, 53.8, 60.5)


def _in_box(lon: float, lat: float, box: Box) -> bool:
    lon_min, lon_max, lat_min, lat_max = box
    return lon_min <= lon <= lon_max and lat_min <= lat <= lat_max


def _in_any(lon: float, lat: float, boxes: Sequence[Box]) -> bool:
    return any(_in_box(lon, lat, b) for b in boxes)


def is_baltic_water(lon: float, lat: float) -> bool:
    if not _in_box(lon, lat, BALTIC_BOUNDS):
        return False
    if _in_any(lon, lat, MAINLAND_BOXES) or _in_any(lon, lat, ISLAND_BOXES):
        return False
    if _in_any(lon, lat, OPEN_WATER_ZONES):
        return True
    # Near an offshore seed (within 45 km) counts as water if not on land/island.
    for slon, slat in OFFSHORE_SEEDS:
        _, _, dist_m = _GEOD.inv(lon, lat, slon, slat)
        if abs(dist_m) <= 45_000:
            return True
    return False


def random_baltic_water_position(
    rng: np.random.Generator,
    max_attempts: int = 128,
) -> Tuple[float, float]:
    for _ in range(max_attempts):
        if float(rng.random()) < 0.65:
            slon, slat = OFFSHORE_SEEDS[int(rng.integers(0, len(OFFSHORE_SEEDS)))]
            dist_m = float(rng.uniform(0.0, 35_000.0))
            az = float(rng.uniform(0.0, 360.0))
            lon, lat, _ = _GEOD.fwd(slon, slat, az, dist_m)
        else:
            zone = OPEN_WATER_ZONES[int(rng.integers(0, len(OPEN_WATER_ZONES)))]
            lon = float(rng.uniform(zone[0], zone[1]))
            lat = float(rng.uniform(zone[2], zone[3]))
        if is_baltic_water(lon, lat):
            return float(lon), float(lat)
    slon, slat = OFFSHORE_SEEDS[0]
    return float(slon), float(slat)


def random_position_near_port_water(
    rng: np.random.Generator,
    port_lon: float,
    port_lat: float,
    min_km: float,
    max_km: float,
    max_attempts: int = 96,
) -> Tuple[float, float]:
    """Offset from a port, biased to seaward bearings, until ``is_baltic_water``."""
    for _ in range(max_attempts):
        dist_m = float(rng.uniform(min_km * 1000.0, max_km * 1000.0))
        # Full circle first; later attempts widen search.
        azimuth = float(rng.uniform(0.0, 360.0))
        lon, lat, _ = _GEOD.fwd(port_lon, port_lat, azimuth, dist_m)
        if is_baltic_water(lon, lat):
            return float(lon), float(lat)
    return random_baltic_water_position(rng)


def _rng(seed: int | None = None) -> np.random.Generator:
    if seed is None:
        seed = int(np.random.randint(0, 2**31))
    return np.random.default_rng(seed)


def sample_baltic_water_position(seed: int | None = None) -> Tuple[float, float]:
    """(lon, lat) on Baltic water — for simulators using the stdlib ``random`` module."""
    return random_baltic_water_position(_rng(seed))


def sample_water_near_harbor(
    harbor_lon: float,
    harbor_lat: float,
    min_km: float = 5.0,
    max_km: float = 25.0,
    seed: int | None = None,
) -> Tuple[float, float]:
    """(lon, lat) in water near a harbor."""
    return random_position_near_port_water(
        _rng(seed), harbor_lon, harbor_lat, min_km, max_km
    )


def snap_to_baltic_water(lon: float, lat: float) -> Tuple[float, float]:
    """Keep position if on water; otherwise return a nearby water point."""
    if is_baltic_water(lon, lat):
        return float(lon), float(lat)
    return sample_water_near_harbor(lon, lat, min_km=2.0, max_km=15.0)
