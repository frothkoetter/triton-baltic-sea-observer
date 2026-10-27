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
    (20.6, 57.15),  # Ventspils roads / W Kurzeme coast
    (20.1, 56.55),  # W of Liepāja
    (21.2, 56.8),
    (21.0, 57.32),  # Irbe Strait (W)
    (21.45, 57.42),  # Irbe Strait (mid)
    (21.9, 57.52),  # Irbe Strait (E)
    (22.3, 57.55),  # Irbe exit / N Kurzeme
    (21.6, 58.0),  # W Moonsund
    (22.5, 58.8),  # E Moonsund / W Hiiumaa
    (23.0, 58.9),  # W of Pakri
    (23.8, 59.0),  # N of Pakri / Tallinn offshore
    (24.5, 59.2),  # Gulf of Finland W lane
    (25.5, 59.9),  # Gulf of Finland mid-channel
    (26.2, 59.6),  # E Gulf of Finland
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
    (29.8, 59.85),  # Kotlin / E Gulf of Finland (Primorsk / SPB approaches)
    (27.2, 60.15),  # W Gulf of Finland mid-channel
    (13.2, 54.6),
    (15.5, 54.8),
    (18.0, 54.5),
    (20.0, 54.8),
    (10.3, 54.45),  # Kiel Bight / western Baltic
    (11.5, 54.15),  # Wismar / Rostock approaches
    (12.4, 54.35),  # Warnemünde / Stralsund lane
    (12.75, 54.52),  # Strelasund (Stralsund–Rügen)
    (13.15, 54.48),  # SW Rügen / Schaproder Bodden lane
    (13.45, 54.42),  # Greifswalder Bodden outer
    (13.25, 54.72),  # N of Darß–Zingst (open Baltic)
    (13.6, 54.55),  # Sassnitz / Rügen sound
]

# Narrow fairways — treated as sea for routing/placement (overrides island boxes).
CHANNEL_BOXES: List[Box] = [
    (21.55, 22.55, 58.02, 58.78),  # Moonsund (Väinameri)
    (22.35, 23.25, 58.32, 58.98),  # Virtsu–Kuivastu / Muhu Väin
    (24.68, 25.35, 59.40, 59.88),  # Tallinn–Helsinki mid-channel (not Harju hinterland)
    (22.85, 23.65, 58.82, 59.18),  # N of Pakri / Paldiski approaches
    (25.35, 27.05, 59.35, 60.25),  # Central Gulf of Finland
    (12.55, 13.18, 54.44, 54.60),  # Strelasund
    (13.02, 13.58, 54.36, 54.52),  # Rügen SW sounds (not island interior)
    (13.52, 14.38, 54.30, 54.54),  # Greifswalder Bodden / Peenestrom approach
    (14.32, 14.78, 54.28, 54.50),  # Swinemünde / Wolin strait
    (12.85, 13.55, 54.70, 54.88),  # Baltic north of Darß–Zingst peninsula
]

# Coastal sea rectangles for random draws only — kept narrow so LV/LT/EE interiors stay land.
OPEN_WATER_ZONES: List[Box] = [
    (15.2, 17.2, 54.55, 55.35),
    (17.0, 20.5, 55.0, 56.8),
    (20.0, 22.8, 56.45, 57.95),  # W Latvia shelf (Ventspils / Liepāja approaches)
    (22.8, 24.9, 56.85, 57.95),  # Gulf of Riga (not all of Latvia)
    (23.5, 25.8, 59.0, 60.05),  # Gulf of Finland west
    (26.0, 30.5, 59.45, 60.48),  # Gulf of Finland east (SPB / Primorsk lanes)
    (11.0, 13.5, 56.0, 57.8),
    (13.5, 15.5, 55.0, 56.2),
    (20.5, 23.8, 57.75, 59.05),  # Irbe / Moonsund / W Estonian lanes
    (24.0, 25.4, 58.35, 59.22),  # N Gulf of Riga → GoF (offshore strip; not Rapla/Tallinn land)
    (24.2, 26.8, 58.15, 59.12),  # E Estonian shelf (not Tartu / Rakke belt)
    (20.8, 22.8, 57.85, 58.55),  # west Estonian shelf
    (9.2, 11.9, 53.82, 54.58),  # German Bight / Kiel Bay (coastal sea)
    (11.9, 14.3, 54.02, 54.72),  # Mecklenburg Bight (coastal sea)
    (12.6, 14.6, 54.72, 55.12),  # Arkona / Zingst offshore
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
    (22.05, 23.05, 58.08, 58.62),  # Saaremaa land core (not Moonsund channels)
    (22.75, 23.45, 58.78, 59.22),  # Hiiumaa land core
    (22.95, 23.35, 58.52, 58.68),  # Muhu land core
    (23.72, 24.30, 57.95, 58.30),  # Kihnu
    (24.62, 25.05, 57.70, 57.98),  # Ruhnu
    (23.15, 23.45, 58.88, 59.08),  # Vormsi
    (13.30, 13.75, 54.95, 55.20),  # Fehmarn
    (12.35, 12.65, 54.82, 55.02),  # Mön
    (13.38, 13.62, 54.30, 54.48),  # Rügen land core (not Strelasund / sounds)
    (13.85, 14.25, 53.85, 54.12),  # Usedom
    (14.45, 14.75, 54.28, 54.48),  # Wolin
    (20.25, 20.75, 54.72, 55.05),  # Kotlin (Kaliningrad Bay)
    (10.90, 11.40, 54.85, 55.10),  # Lolland (sounds)
]

# Mainland / large coastal land (conservative; avoids Sweden, Finland, PL, DE, DK).
MAINLAND_BOXES: List[Box] = [
    (9.9, 12.4, 53.5, 53.82),  # Schleswig-Holstein / MV inland (not Kiel/Rostock bays)
    (12.0, 13.9, 53.52, 53.92),  # Mecklenburg hinterland (not coastal lane)
    (10.5, 16.8, 55.45, 58.85),
    (16.8, 18.05, 57.8, 60.6),
    (18.05, 21.5, 59.2, 60.6),
    (29.8, 30.4, 60.42, 60.5),  # NE Karelian coast (excludes Primorsk / Kotlin bays below 60.4°N)
    (14.0, 19.5, 53.5, 54.55),
    (19.5, 21.8, 53.8, 54.55),
    (21.8, 24.5, 53.9, 55.8),
    (24.5, 28.5, 54.0, 57.5),
    (12.25, 14.35, 54.50, 54.70),  # Darß–Zingst–Rügen hinterland (not sea N of peninsula)
    # Baltic states — inland cores only (gulfs/bays stay water via offshore seeds)
    (24.2, 26.2, 58.35, 59.12),
    (25.4, 27.9, 58.15, 58.95),
    (25.0, 27.3, 59.12, 59.55),
    (24.45, 25.25, 59.28, 59.52),
    (23.6, 26.8, 55.85, 56.85),
    (22.2, 25.8, 54.15, 55.75),
    (25.0, 27.2, 57.35, 58.05),  # N Latvia inland (e.g. Valmiera)
    (21.5, 24.9, 56.15, 56.85),  # Latvia / Kurzeme inland (S)
    (22.05, 23.55, 56.88, 57.52),  # Kurzeme hinterland (inland; not Irbe / Ventspils bay)
    (21.5, 24.0, 55.45, 56.25),  # Lithuania inland (Telšių area)
    (26.0, 27.9, 58.05, 58.75),  # SE Estonia inland (Tartu belt)
    (18.0, 19.8, 54.55, 55.05),  # Kashubian coast hinterland (N of Gdynia)
    (27.9, 29.35, 57.55, 58.25),  # Pskov / upper Velikaya (inland; not a sea lane)
    (9.95, 10.55, 54.18, 54.38),  # Schleswig lakeland W of Kiel (not Baltic Sea)
    (12.05, 12.55, 54.08, 54.22),  # Mecklenburg hinterland NE of Rostock
    (25.85, 27.25, 58.82, 59.28),  # E Estonia inland (e.g. Rakke; not Peipus coast lane)
    (23.38, 25.05, 58.68, 59.46),  # W & central Estonia (Rapla, Keila, Haapsalu hinterland)
    (25.05, 27.15, 59.12, 59.48),  # N Estonia inland (Rakvere, Jõhvi belt; not GoF lane)
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
    if _in_any(lon, lat, CHANNEL_BOXES):
        return True
    if _in_any(lon, lat, OPEN_WATER_ZONES):
        # Coastal zone rectangles overlap lakes/hinterland — require a nearby offshore seed.
        for slon, slat in OFFSHORE_SEEDS:
            _, _, dist_m = _GEOD.inv(lon, lat, slon, slat)
            if abs(dist_m) <= SEA_SEED_MAX_M:
                return True
        return False
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
            dist_m = float(rng.uniform(0.0, 22_000.0))
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
    """Bearing from port toward open water (not the geographically nearest seed)."""
    best_az = 270.0
    best_score = -1.0
    for slon, slat in OFFSHORE_SEEDS:
        if not is_baltic_water(slon, slat):
            continue
        az, _, dist_m = _GEOD.inv(port_lon, port_lat, slon, slat)
        d = abs(dist_m)
        if d < 25_000.0:
            continue
        probe_lon, probe_lat, _ = _GEOD.fwd(port_lon, port_lat, az, min(d * 0.12, 12_000.0))
        if not is_baltic_water(probe_lon, probe_lat):
            continue
        if d > best_score:
            best_score = d
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
    max_m = max_km * 1000.0 * 1.05
    min_m = min_km * 1000.0
    seaward = _seaward_azimuth_deg(port_lon, port_lat)

    def _accept(lon: float, lat: float) -> bool:
        if not is_baltic_water(lon, lat):
            return False
        _, _, dist_m = _GEOD.inv(port_lon, port_lat, lon, lat)
        return min_m <= abs(dist_m) <= max_m

    for attempt in range(max_attempts):
        dist_m = float(rng.uniform(min_m, max_km * 1000.0))
        if attempt < max_attempts * 2 // 3:
            azimuth = seaward + float(rng.uniform(-55.0, 55.0))
        else:
            azimuth = float(rng.uniform(0.0, 360.0))
        lon, lat, _ = _GEOD.fwd(port_lon, port_lat, azimuth, dist_m)
        if _accept(lon, lat):
            return float(lon), float(lat)

    for dist_m in (min_m, (min_m + max_km * 1000.0) / 2.0, max_km * 1000.0):
        for azimuth in np.linspace(0.0, 360.0, 24, endpoint=False):
            lon, lat, _ = _GEOD.fwd(port_lon, port_lat, float(azimuth), float(dist_m))
            if _accept(lon, lat):
                return float(lon), float(lat)

    lon, lat, _ = _GEOD.fwd(port_lon, port_lat, seaward, min_m)
    if is_baltic_water(lon, lat):
        return float(lon), float(lat)
    return snap_to_baltic_water(port_lon, port_lat)


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
    rng = _rng(seed)
    for cap_km in (max_km, max(max_km, 35.0), 50.0):
        lon, lat = random_position_near_port_water(
            rng, harbor_lon, harbor_lat, min_km, cap_km
        )
        if is_baltic_water(lon, lat):
            _, _, dist_m = _GEOD.inv(harbor_lon, harbor_lat, lon, lat)
            if abs(dist_m) <= cap_km * 1000.0 * 1.08:
                return lon, lat
    return random_position_near_port_water(rng, harbor_lon, harbor_lat, min_km, 50.0)


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
