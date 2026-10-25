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

# Coastal sea rectangles for random draws only — kept narrow so LV/LT/EE interiors stay land.
OPEN_WATER_ZONES: List[Box] = [
    (15.2, 17.2, 54.55, 55.35),
    (17.0, 20.5, 55.0, 56.8),
    (22.8, 24.9, 56.85, 57.95),  # Gulf of Riga (not all of Latvia)
    (23.5, 25.8, 59.0, 60.05),  # Gulf of Finland west
    (11.0, 13.5, 56.0, 57.8),
    (13.5, 15.5, 55.0, 56.2),
    (20.8, 22.8, 57.85, 58.55),  # west Estonian shelf
]

# Lakes / river mouths — wet but not Baltic Sea AIS/MAD buoy placement.
INLAND_WATER_BOXES: List[Box] = [
    (25.15, 28.4, 57.55, 58.95),  # Lake Peipus + Pskov approach
]

# Islands and archipelago cores (exclude; jitter from seeds may land here).
ISLAND_BOXES: List[Box] = [
    (14.70, 15.35, 54.95, 55.40),  # Bornholm
    (15.05, 15.45, 55.18, 55.38),  # Ertholmene (east of Bornholm)
    (17.70, 19.40, 56.85, 57.95),  # Gotland (+ Fårö)
    (19.05, 19.55, 58.15, 58.55),  # Gotska Sandön (N of Gotland)
    (16.55, 17.25, 56.15, 56.55),  # Öland
    (19.35, 21.15, 59.75, 60.35),  # Åland archipelago
    (20.85, 21.25, 59.85, 60.15),  # Kökar / outer Åland
    (21.85, 23.20, 57.90, 58.78),  # Saaremaa (+ Abruka)
    (22.55, 23.70, 58.62, 59.38),  # Hiiumaa
    (22.85, 23.45, 58.45, 58.75),  # Muhu
    (23.72, 24.30, 57.95, 58.30),  # Kihnu
    (24.62, 25.05, 57.70, 57.98),  # Ruhnu
    (23.15, 23.45, 58.88, 59.08),  # Vormsi
    (13.30, 13.75, 54.95, 55.20),  # Fehmarn
    (12.35, 12.65, 54.82, 55.02),  # Mön
    (13.25, 13.75, 54.25, 54.55),  # Rügen
    (13.85, 14.25, 53.85, 54.12),  # Usedom
    (14.45, 14.75, 54.28, 54.48),  # Wolin
    (20.25, 20.75, 54.72, 55.05),  # Kotlin (Kaliningrad Bay)
    (10.90, 11.40, 54.85, 55.10),  # Lolland (sounds)
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
    # Baltic states — inland cores only (gulfs/bays stay water via offshore seeds)
    (24.2, 26.2, 58.35, 59.12),
    (25.4, 27.9, 58.15, 58.95),
    (25.0, 27.3, 59.12, 59.55),
    (24.45, 25.25, 59.28, 59.52),
    (23.6, 26.8, 55.85, 56.85),
    (22.2, 25.8, 54.15, 55.75),
    (25.0, 27.2, 57.35, 58.05),  # N Latvia inland (e.g. Valmiera)
    (21.5, 24.9, 56.15, 56.85),  # Latvia / Kurzeme inland (S)
    (21.05, 23.55, 56.78, 57.58),  # Kurzeme hinterland (Dundaga, Valdemārpils, E of Pāvilosta)
    (21.5, 24.0, 55.45, 56.25),  # Lithuania inland (Telšių area)
    (26.0, 27.9, 58.05, 58.75),  # SE Estonia inland (Tartu belt)
    (18.0, 19.8, 54.55, 55.05),  # Kashubian coast hinterland (N of Gdynia)
]

BALTIC_BOUNDS: Box = (10.0, 30.0, 53.8, 60.5)
SEA_SEED_MAX_M = 30_000  # max distance to offshore seed to count as sea (not lakes/inland)


def _in_box(lon: float, lat: float, box: Box) -> bool:
    lon_min, lon_max, lat_min, lat_max = box
    return lon_min <= lon <= lon_max and lat_min <= lat <= lat_max


def _in_any(lon: float, lat: float, boxes: Sequence[Box]) -> bool:
    return any(_in_box(lon, lat, b) for b in boxes)


def is_baltic_water(lon: float, lat: float) -> bool:
    """Sea in the Baltic basin; inland EE/LV/LT cores are excluded before open-water zones."""
    if not _in_box(lon, lat, BALTIC_BOUNDS):
        return False
    if (
        _in_any(lon, lat, MAINLAND_BOXES)
        or _in_any(lon, lat, ISLAND_BOXES)
        or _in_any(lon, lat, INLAND_WATER_BOXES)
    ):
        return False
    if _in_any(lon, lat, OPEN_WATER_ZONES):
        return True
    for slon, slat in OFFSHORE_SEEDS:
        _, _, dist_m = _GEOD.inv(lon, lat, slon, slat)
        if abs(dist_m) <= SEA_SEED_MAX_M:
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


def _seaward_azimuth_deg(port_lon: float, port_lat: float) -> float:
    """Bearing from port toward the nearest offshore seed (open water)."""
    best_az = 0.0
    best_dist = 1e18
    for slon, slat in OFFSHORE_SEEDS:
        az, _, dist_m = _GEOD.inv(port_lon, port_lat, slon, slat)
        d = abs(dist_m)
        if d < best_dist:
            best_dist = d
            best_az = az
    return float(best_az)


def random_position_near_port_water(
    rng: np.random.Generator,
    port_lon: float,
    port_lat: float,
    min_km: float,
    max_km: float,
    max_attempts: int = 96,
) -> Tuple[float, float]:
    """Offset from a port toward open water until ``is_baltic_water``."""
    seaward = _seaward_azimuth_deg(port_lon, port_lat)
    for attempt in range(max_attempts):
        dist_m = float(rng.uniform(min_km * 1000.0, max_km * 1000.0))
        if attempt < max_attempts * 2 // 3:
            azimuth = seaward + float(rng.uniform(-70.0, 70.0))
        else:
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
    """Keep position if on water; otherwise move toward the nearest offshore seed."""
    if is_baltic_water(lon, lat):
        return float(lon), float(lat)
    best_lon, best_slat = OFFSHORE_SEEDS[0]
    best_dist = 1e18
    for slon, slat in OFFSHORE_SEEDS:
        _, _, dist_m = _GEOD.inv(lon, lat, slon, slat)
        d = abs(dist_m)
        if d < best_dist:
            best_dist = d
            best_lon, best_slat = slon, slat
    # Step from land toward open water along the bearing to the nearest seed.
    fwd_az, _, dist_m = _GEOD.inv(lon, lat, best_lon, best_slat)
    step_m = min(max(dist_m * 0.85, 15_000.0), 120_000.0)
    snap_lon, snap_lat, _ = _GEOD.fwd(lon, lat, fwd_az, step_m)
    if is_baltic_water(snap_lon, snap_lat):
        return float(snap_lon), float(snap_lat)
    return sample_baltic_water_position()
