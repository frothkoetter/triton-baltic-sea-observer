"""
NiFi LiDAR simulator: parametric Baltic fleet scene exported as LAS (EPSG:4326).

Legacy box/submarine/sailing modes are replaced by analytic hull + superstructure
sampling; see lidar_vessel_geometry.py for vessel builders.
"""

from __future__ import annotations

import io
import os
import tempfile
from typing import List, Optional, Tuple

import laspy
import numpy as np
from pyproj import CRS, Geod

from lidar_vessel_geometry import VesselSpec, baltic_fleet_scene, vessel_local_points

try:
    from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
    from nifiapi.properties import PropertyDescriptor, StandardValidators

    _NIFI_AVAILABLE = True
except ImportError:  # local / unit test without NiFi runtime
    _NIFI_AVAILABLE = False

_GEOD = Geod(ellps="WGS84")


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
    type_codes = {
        "oil_tanker": 1,
        "gas_tanker": 2,
        "container_ship": 3,
        "ferry": 4,
        "sailing": 5,
        "corvette": 6,
        "frigate": 7,
        "submarine": 8,
        "landing_ship": 9,
    }
    for idx, spec in enumerate(fleet):
        east = (idx % 5) * 450.0
        north = (idx // 5) * 520.0
        center_lon, center_lat = _offset_position(base_lon, base_lat, east, north)
        local = vessel_local_points(spec, rng)
        geo = _local_to_geographic(local, center_lon, center_lat, spec.heading_deg)
        all_pts.append(geo)
        code = type_codes.get(spec.vessel_type, 0)
        all_cls.append(np.full(len(geo), code, dtype=np.uint16))
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
        points, classification = build_scene_points(base_lon, base_lat, seed=seed)
    header = laspy.LasHeader(point_format=3, version="1.4")
    header.add_crs(CRS.from_epsg(4326))
    header.scales = [1e-7, 1e-7, 0.01]
    header.offsets = [base_lon, base_lat, 0.0]

    las = laspy.LasData(header)
    las.x, las.y, las.z = points[:, 0], points[:, 1], points[:, 2]
    las.classification = classification
    las.intensity = np.clip(80 + classification * 7, 0, 255).astype(np.uint16)
    las.write(filename)
    return filename


if _NIFI_AVAILABLE:

    class LidarSimulator(FlowFileTransform):
        class Java:
            implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

        class ProcessorDetails:
            version = "3.0.0"
            description = (
                "Generates a Baltic Sea LiDAR scene (LAS 1.4, EPSG:4326) with parametric "
                "oil/gas tankers, container ship, ferry, sailing yacht, and Russian Navy "
                "surface/combat logistics silhouettes (corvette, frigate, submarine, LST)."
            )
            dependencies = ["numpy", "laspy", "pyproj"]

        BASE_LATITUDE = PropertyDescriptor(
            name="Base Latitude",
            description="Scene anchor latitude (degrees).",
            validators=[StandardValidators.NUMBER_VALIDATOR],
            default_value="59.0",
            required=True,
        )
        BASE_LONGITUDE = PropertyDescriptor(
            name="Base Longitude",
            description="Scene anchor longitude (degrees).",
            validators=[StandardValidators.NUMBER_VALIDATOR],
            default_value="19.0",
            required=True,
        )
        RANDOM_SEED = PropertyDescriptor(
            name="Random Seed",
            description="Optional seed for reproducible point placement (empty = nondeterministic).",
            validators=[StandardValidators.INTEGER_VALIDATOR],
            required=False,
        )
        OUTPUT_BASENAME = PropertyDescriptor(
            name="Output Basename",
            description="Filename for the generated .las file in the flowfile temp directory.",
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            default_value="baltic_fleet.las",
            required=True,
        )

        def __init__(self, **kwargs):
            kwargs.pop("jvm", None)
            super().__init__(**kwargs)
            self.descriptors = [
                self.BASE_LATITUDE,
                self.BASE_LONGITUDE,
                self.RANDOM_SEED,
                self.OUTPUT_BASENAME,
            ]

        def getPropertyDescriptors(self):
            return self.descriptors

        def transform(self, context, flowfile):
            base_lat = float(context.getProperty(self.BASE_LATITUDE))
            base_lon = float(context.getProperty(self.BASE_LONGITUDE))
            seed_raw = context.getProperty(self.RANDOM_SEED)
            seed = int(seed_raw) if seed_raw else None
            basename = context.getProperty(self.OUTPUT_BASENAME)
            if not basename.lower().endswith(".las"):
                basename = f"{basename}.las"

            tmp_dir = tempfile.mkdtemp(prefix="lidar_sim_")
            out_path = os.path.join(tmp_dir, basename)
            points, classification = build_scene_points(base_lon, base_lat, seed=seed)
            write_baltic_lidar(
                out_path,
                base_lon=base_lon,
                base_lat=base_lat,
                points=points,
                classification=classification,
            )

            with open(out_path, "rb") as f:
                payload = f.read()

            attrs = {
                "mime.type": "application/octet-stream",
                "lidar.point_count": str(len(points)),
                "lidar.filename": basename,
                "lidar.base.lat": str(base_lat),
                "lidar.base.lon": str(base_lon),
            }

            return FlowFileTransformResult(
                relationship="success",
                contents=io.BytesIO(payload),
                attributes=attrs,
            )


if __name__ == "__main__":
    write_baltic_lidar("baltic_fleet.las")
    print("Generated Baltic Sea LiDAR: baltic_fleet.las (EPSG:4326)")
