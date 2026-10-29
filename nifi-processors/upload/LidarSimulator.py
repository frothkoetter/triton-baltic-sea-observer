"""
NiFi LiDAR simulator: parametric Baltic fleet scene exported as LAS (EPSG:4326).

Legacy box/submarine/sailing modes are replaced by analytic hull + superstructure
sampling; see lidar_vessel_geometry.py for vessel builders.

Local run (requires laspy, pyproj — see nifi-processors/requirements.txt):

    python3 -m pip install -r requirements.txt
    python3 LidarSimulator.py
"""

from __future__ import annotations

import os
import sys
import tempfile
from dataclasses import replace
from pathlib import Path
from typing import List, Optional, Tuple

# NiFi loads this file from python/extensions but runs the processor in an isolated
# venv; sibling modules are only importable if the extensions dir is on sys.path.
_EXTENSIONS_DIR = Path(__file__).resolve().parent
if _EXTENSIONS_DIR.as_posix() not in sys.path:
    sys.path.insert(0, _EXTENSIONS_DIR.as_posix())

import laspy
import numpy as np
from pyproj import CRS, Geod, Transformer

from baltic_water_placement import (
    is_baltic_water,
    random_baltic_water_position,
    random_position_near_port_water,
)
from lidar_sea_surface import LAS_CLASS_WATER_SURFACE, SeaSurfaceConfig, sample_sparse_sea_surface_local
from lidar_vessel_geometry import (
    LAS_CLASS_NEVER_CLASSIFIED,
    VESSEL_TYPE_CODES,
    VesselSpec,
    baltic_fleet_scene,
    vessel_local_points,
)

try:
    from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
    from nifiapi.properties import PropertyDescriptor, StandardValidators
except ImportError:  # local CLI only — NiFi always provides nifiapi
    FlowFileTransform = object  # type: ignore[misc, assignment]
    FlowFileTransformResult = None  # type: ignore[misc, assignment]

    def PropertyDescriptor(**_kwargs):  # type: ignore[misc]
        return None

    class StandardValidators:  # type: ignore[no-redef]
        NUMBER_VALIDATOR = INTEGER_VALIDATOR = NON_EMPTY_VALIDATOR = None

_GEOD = Geod(ellps="WGS84")

# Major Baltic ports (WGS84) — unclassified contacts are placed 5–20 km offshore.
BALTIC_PORTS: List[Tuple[str, float, float]] = [
    ("Hamburg", 9.973, 53.551),
    ("Rostock", 12.091, 54.088),
    ("Kiel", 10.122, 54.323),
    ("Lübeck", 10.687, 53.869),
    ("Gdansk", 18.660, 54.352),
    ("Gdynia", 18.551, 54.530),
    ("Szczecin", 14.552, 53.428),
    ("Copenhagen", 12.568, 55.676),
    ("Malmö", 13.004, 55.605),
    ("Gothenburg", 11.974, 57.708),
    ("Stockholm", 18.068, 59.329),
    ("Helsinki", 24.938, 60.170),
    ("Tallinn", 24.754, 59.437),
    ("Riga", 24.105, 56.949),
    ("Klaipeda", 21.135, 55.703),
    ("Kaliningrad", 20.510, 54.710),
    ("Saint Petersburg", 30.260, 59.934),
    ("Turku", 22.228, 60.451),
    ("Aarhus", 10.210, 56.150),
    ("Esbjerg", 8.451, 55.467),
]

UNCLASSIFIED_FRACTION = 0.10
DEFAULT_SEA_SURFACE_BUDGET = 1_200
PORT_OFFSET_MIN_KM = 5.0
PORT_OFFSET_MAX_KM = 20.0
UNDEFINED_VESSEL_LABEL = "undefined"


def _local_to_geographic(
    points_local: np.ndarray,
    center_lon: float,
    center_lat: float,
    heading_deg: float,
) -> np.ndarray:
    """
    Map vessel frame (x bow, y starboard, z up) to WGS84 lon/lat/z meters AMSL.
    """
    if len(points_local) == 0:
        return np.empty((0, 3), dtype=np.float64)
    h = np.radians(heading_deg)
    east = points_local[:, 0] * np.sin(h) + points_local[:, 1] * np.cos(h)
    north = points_local[:, 0] * np.cos(h) - points_local[:, 1] * np.sin(h)
    dist = np.hypot(east, north)
    az = np.degrees(np.arctan2(east, north))
    lon, lat, _ = _GEOD.fwd(
        np.full(len(points_local), center_lon),
        np.full(len(points_local), center_lat),
        az,
        dist,
    )
    return np.column_stack((lon, lat, points_local[:, 2]))


def _offset_position(
    base_lon: float,
    base_lat: float,
    east_m: float,
    north_m: float,
) -> Tuple[float, float]:
    lon, lat, _ = _GEOD.fwd(base_lon, base_lat, 90.0, east_m)
    lon, lat, _ = _GEOD.fwd(lon, lat, 0.0, north_m)
    return lon, lat


def _random_position_near_baltic_port(
    rng: np.random.Generator,
    min_km: float = PORT_OFFSET_MIN_KM,
    max_km: float = PORT_OFFSET_MAX_KM,
) -> Tuple[float, float]:
    """Random WGS84 water point min_km–max_km from a random major Baltic port."""
    _name, port_lon, port_lat = BALTIC_PORTS[int(rng.integers(0, len(BALTIC_PORTS)))]
    return random_position_near_port_water(rng, port_lon, port_lat, min_km, max_km)


def _sample_scene_center(
    rng: np.random.Generator,
    near_port: bool,
    max_attempts: int = 64,
) -> Tuple[float, float]:
    """WGS84 scene center guaranteed on Baltic sea (not lakes / coastal hinterland)."""
    for _ in range(max_attempts):
        if near_port:
            lon, lat = _random_position_near_baltic_port(rng)
        else:
            lon, lat = random_baltic_water_position(rng)
        if is_baltic_water(lon, lat):
            return lon, lat
    lon, lat = random_baltic_water_position(rng)
    return lon, lat


def build_single_vessel_points(
    seed: Optional[int] = None,
    unclassified_fraction: float = UNCLASSIFIED_FRACTION,
    sea_surface_budget: int = DEFAULT_SEA_SURFACE_BUDGET,
    wave_amplitude_m: float = 0.6,
    wave_length_m: float = 18.0,
) -> Tuple[np.ndarray, np.ndarray, VesselSpec, float, float, bool]:
    """
    Pick one vessel archetype, random heading, and Baltic position.

    With probability ``unclassified_fraction`` (default 10%): LAS class 0 (never
    classified), center 5–20 km from a Baltic port, metadata vessel type/name
    ``undefined``. Otherwise random open-sea position and normal class 1–9.
    """
    rng = np.random.default_rng(seed)
    templates = baltic_fleet_scene()
    template = templates[int(rng.integers(0, len(templates)))]
    is_unclassified = float(rng.random()) < float(unclassified_fraction)
    center_lon, center_lat = _sample_scene_center(rng, near_port=is_unclassified)
    heading = float(rng.uniform(0.0, 360.0))
    spec = replace(template, heading_deg=heading)
    local = vessel_local_points(spec, rng)
    sea_local = sample_sparse_sea_surface_local(
        spec,
        rng,
        SeaSurfaceConfig(
            point_budget=sea_surface_budget,
            wave_amplitude_m=wave_amplitude_m,
            wave_length_m=wave_length_m,
        ),
    )
    if len(sea_local):
        local = np.vstack((local, sea_local))
    geo = _local_to_geographic(local, center_lon, center_lat, spec.heading_deg)
    if is_unclassified:
        code = LAS_CLASS_NEVER_CLASSIFIED
    else:
        code = VESSEL_TYPE_CODES.get(spec.vessel_type, LAS_CLASS_NEVER_CLASSIFIED)
    n_hull = len(geo) - len(sea_local)
    classification = np.empty(len(geo), dtype=np.uint16)
    if n_hull > 0:
        classification[:n_hull] = code
    if len(sea_local):
        classification[n_hull:] = LAS_CLASS_WATER_SURFACE
    return geo, classification, spec, center_lon, center_lat, is_unclassified


def build_scene_points(
    base_lon: float = 19.0,
    base_lat: float = 59.0,
    fleet: Optional[List[VesselSpec]] = None,
    seed: Optional[int] = None,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Returns (N,3) geographic points and per-point classification codes (uint16).
    """
    rng = np.random.default_rng(seed)
    fleet = fleet or baltic_fleet_scene()
    all_pts: List[np.ndarray] = []
    all_cls: List[np.ndarray] = []
    for idx, spec in enumerate(fleet):
        east = (idx % 5) * 450.0
        north = (idx // 5) * 520.0
        center_lon, center_lat = _offset_position(base_lon, base_lat, east, north)
        local = vessel_local_points(spec, rng)
        geo = _local_to_geographic(local, center_lon, center_lat, spec.heading_deg)
        all_pts.append(geo)
        code = VESSEL_TYPE_CODES.get(spec.vessel_type, 0)
        all_cls.append(np.full(len(geo), code, dtype=np.uint16))
    points = np.vstack(all_pts) if all_pts else np.empty((0, 3))
    classification = np.concatenate(all_cls) if all_cls else np.empty(0, dtype=np.uint16)
    return points, classification


def _utm_anchor(base_lon: float, base_lat: float, utm_epsg: int = 32635) -> Tuple[float, float]:
    to_utm = Transformer.from_crs("EPSG:4326", f"EPSG:{utm_epsg}", always_xy=True)
    return to_utm.transform(base_lon, base_lat)


def vessel_spec_to_enu(
    spec: VesselSpec,
    rng: np.random.Generator,
    anchor_e: float,
    anchor_n: float,
    offset_east: float = 0.0,
    offset_north: float = 0.0,
    heading_deg: Optional[float] = None,
) -> np.ndarray:
    """Single vessel point cloud in UTM meters (bow along +north when heading_deg=0)."""
    local = vessel_local_points(spec, rng)
    h = np.radians(heading_deg if heading_deg is not None else spec.heading_deg)
    east = local[:, 0] * np.sin(h) + local[:, 1] * np.cos(h)
    north = local[:, 0] * np.cos(h) - local[:, 1] * np.sin(h)
    x = anchor_e + offset_east + east
    y = anchor_n + offset_north + north
    return np.column_stack((x, y, local[:, 2]))


def build_scene_points_enu(
    base_lon: float = 19.0,
    base_lat: float = 59.0,
    fleet: Optional[List[VesselSpec]] = None,
    seed: Optional[int] = None,
    utm_epsg: int = 32635,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Same fleet as build_scene_points, but x/y are UTM meters (z still meters AMSL).
    Use this file for 3D viewers — EPSG:4326 lon/lat on x/y squashes ship shapes.
    """
    rng = np.random.default_rng(seed)
    fleet = fleet or baltic_fleet_scene()
    to_utm = Transformer.from_crs("EPSG:4326", f"EPSG:{utm_epsg}", always_xy=True)
    anchor_e, anchor_n = to_utm.transform(base_lon, base_lat)

    all_pts: List[np.ndarray] = []
    all_cls: List[np.ndarray] = []
    for idx, spec in enumerate(fleet):
        fleet_east = (idx % 5) * 450.0
        fleet_north = (idx // 5) * 520.0
        pts = vessel_spec_to_enu(spec, rng, anchor_e, anchor_n, fleet_east, fleet_north)
        all_pts.append(pts)
        code = VESSEL_TYPE_CODES.get(spec.vessel_type, 0)
        all_cls.append(np.full(len(pts), code, dtype=np.uint16))
    points = np.vstack(all_pts) if all_pts else np.empty((0, 3))
    classification = np.concatenate(all_cls) if all_cls else np.empty(0, dtype=np.uint16)
    return points, classification


def write_baltic_lidar(
    filename: str,
    base_lon: float = 19.0,
    base_lat: float = 59.0,
    seed: Optional[int] = None,
    points: Optional[np.ndarray] = None,
    classification: Optional[np.ndarray] = None,
) -> str:
    if points is None or classification is None:
        points, classification, _, center_lon, center_lat, _ = build_single_vessel_points(
            seed=seed
        )
        base_lon, base_lat = center_lon, center_lat
    header = laspy.LasHeader(point_format=3, version="1.4")
    header.add_crs(CRS.from_epsg(4326))
    header.scales = [1e-7, 1e-7, 0.01]
    header.offsets = [
        float(np.mean(points[:, 0])),
        float(np.mean(points[:, 1])),
        0.0,
    ]

    las = laspy.LasData(header)
    las.x, las.y, las.z = points[:, 0], points[:, 1], points[:, 2]
    las.classification = classification
    las.intensity = np.clip(80 + classification * 7, 0, 255).astype(np.uint16)
    las.write(filename)
    return filename


def write_las_enu(
    filename: str,
    points: np.ndarray,
    classification: np.ndarray,
    utm_epsg: int = 32635,
) -> str:
    """Write a LAS file with x/y/z in UTM meters (EPSG:32635 by default)."""
    header = laspy.LasHeader(point_format=3, version="1.4")
    header.add_crs(CRS.from_epsg(utm_epsg))
    header.scales = [0.01, 0.01, 0.01]
    header.offsets = [float(points[:, 0].mean()), float(points[:, 1].mean()), 0.0]

    las = laspy.LasData(header)
    las.x, las.y, las.z = points[:, 0], points[:, 1], points[:, 2]
    las.classification = classification
    las.intensity = np.clip(80 + classification * 7, 0, 255).astype(np.uint16)
    las.write(filename)
    return filename


def write_baltic_lidar_enu(
    filename: str,
    base_lon: float = 19.0,
    base_lat: float = 59.0,
    seed: Optional[int] = None,
    utm_epsg: int = 32635,
    points: Optional[np.ndarray] = None,
    classification: Optional[np.ndarray] = None,
) -> str:
    """LAS in UTM meters — recommended for CloudCompare / matplotlib 3D."""
    if points is None or classification is None:
        points, classification, _, _, _, _ = build_single_vessel_points(seed=seed)
        to_utm = Transformer.from_crs("EPSG:4326", f"EPSG:{utm_epsg}", always_xy=True)
        e, n = to_utm.transform(points[:, 0], points[:, 1])
        points = np.column_stack((e, n, points[:, 2]))
    return write_las_enu(filename, points, classification, utm_epsg=utm_epsg)


class LidarSimulator(FlowFileTransform):
    """NiFi Python processor — class must be at module scope for extension discovery."""

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "4.3.0"
        description = (
            "Generates one random parametric vessel as LAS 1.4 EPSG:4326. "
            "Positions are offshore Baltic water only. "
            "Optional sparse sea-surface points (class 10) around the hull. "
            "~10% unclassified (class 0) near ports (5–20 km)."
        )
        dependencies = ["numpy", "laspy", "pyproj"]

    RANDOM_SEED = PropertyDescriptor(
        name="Random Seed",
        description="Optional seed for vessel type, position, heading, and points (empty = random each run).",
        validators=[StandardValidators.INTEGER_VALIDATOR],
        required=False,
    )
    OUTPUT_BASENAME = PropertyDescriptor(
        name="Output Basename",
        description="Filename for the generated .las file in the flowfile temp directory.",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value="baltic_vessel.las",
        required=True,
    )
    UNCLASSIFIED_FRACTION = PropertyDescriptor(
        name="Unclassified Fraction",
        description=(
            "Probability (0–1) of emitting LAS class 0 with vessel_type/name undefined "
            "and position 5–20 km from a Baltic port."
        ),
        validators=[StandardValidators.NUMBER_VALIDATOR],
        default_value="0.1",
        required=False,
    )
    SEA_SURFACE_BUDGET = PropertyDescriptor(
        name="Sea Surface Point Budget",
        description=(
            "Max extra LAS points for water/waves around the vessel (class 10). "
            "0 disables sea surface (hull only)."
        ),
        validators=[StandardValidators.INTEGER_VALIDATOR],
        default_value=str(DEFAULT_SEA_SURFACE_BUDGET),
        required=False,
    )
    WAVE_AMPLITUDE_M = PropertyDescriptor(
        name="Wave Amplitude (m)",
        description="Swell height for sparse sea-surface points (local z ripple).",
        validators=[StandardValidators.NUMBER_VALIDATOR],
        default_value="0.6",
        required=False,
    )

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = [
            self.RANDOM_SEED,
            self.OUTPUT_BASENAME,
            self.UNCLASSIFIED_FRACTION,
            self.SEA_SURFACE_BUDGET,
            self.WAVE_AMPLITUDE_M,
        ]

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, flowfile):
        seed_raw = context.getProperty(self.RANDOM_SEED.name).getValue()
        seed = int(seed_raw) if seed_raw and str(seed_raw).strip() else None
        basename = context.getProperty(self.OUTPUT_BASENAME.name).getValue()
        if not basename.lower().endswith(".las"):
            basename = f"{basename}.las"

        frac_raw = context.getProperty(self.UNCLASSIFIED_FRACTION.name).getValue()
        unclassified_fraction = (
            float(frac_raw) if frac_raw and str(frac_raw).strip() else UNCLASSIFIED_FRACTION
        )
        budget_raw = context.getProperty(self.SEA_SURFACE_BUDGET.name).getValue()
        sea_budget = (
            int(budget_raw)
            if budget_raw and str(budget_raw).strip()
            else DEFAULT_SEA_SURFACE_BUDGET
        )
        amp_raw = context.getProperty(self.WAVE_AMPLITUDE_M.name).getValue()
        wave_amp = float(amp_raw) if amp_raw and str(amp_raw).strip() else 0.6

        points, classification, spec, center_lon, center_lat, is_unclassified = (
            build_single_vessel_points(
                seed=seed,
                unclassified_fraction=unclassified_fraction,
                sea_surface_budget=max(0, sea_budget),
                wave_amplitude_m=max(0.0, wave_amp),
            )
        )
        with tempfile.TemporaryDirectory(prefix="lidar_sim_") as tmp_dir:
            out_path = os.path.join(tmp_dir, basename)
            write_baltic_lidar(
                out_path,
                points=points,
                classification=classification,
            )
            with open(out_path, "rb") as f:
                payload = f.read()

        if is_unclassified:
            vessel_type = UNDEFINED_VESSEL_LABEL
            vessel_name = UNDEFINED_VESSEL_LABEL
            class_code = LAS_CLASS_NEVER_CLASSIFIED
        else:
            vessel_type = spec.vessel_type
            vessel_name = spec.name
            class_code = VESSEL_TYPE_CODES.get(spec.vessel_type, LAS_CLASS_NEVER_CLASSIFIED)

        sea_count = int(np.sum(classification == LAS_CLASS_WATER_SURFACE))
        attrs = {
            "mime.type": "application/octet-stream",
            "lidar.point_count": str(len(points)),
            "lidar.sea_surface.point_count": str(sea_count),
            "lidar.filename": basename,
            "lidar.vessel_type": vessel_type,
            "lidar.vessel_name": vessel_name,
            "lidar.center.lat": str(center_lat),
            "lidar.center.lon": str(center_lon),
            "lidar.heading_deg": str(spec.heading_deg),
            "lidar.classification": str(class_code),
        }

        return FlowFileTransformResult(
            relationship="success",
            contents=payload,
            attributes=attrs,
        )


if __name__ == "__main__":
    write_baltic_lidar("baltic_vessel.las")
    write_baltic_lidar_enu("baltic_vessel_enu.las")
    pts, _, spec, lon, lat, uncl = build_single_vessel_points(seed=42)
    label = UNDEFINED_VESSEL_LABEL if uncl else f"{spec.name} ({spec.vessel_type})"
    print(f"Generated random vessel: {label} at {lat:.4f}N {lon:.4f}E")
    print("  baltic_vessel.las     — EPSG:4326 (NiFi / geo pipelines)")
    print("  baltic_vessel_enu.las — EPSG:32635 UTM meters (CloudCompare / 3D view)")
