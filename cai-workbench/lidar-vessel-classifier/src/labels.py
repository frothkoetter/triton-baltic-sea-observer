"""LAS classification codes (matches nifi-processors/lidar_vessel_geometry.VESSEL_TYPE_CODES)."""

VESSEL_TYPE_CODES = {
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

CODE_TO_VESSEL_TYPE = {code: name for name, code in VESSEL_TYPE_CODES.items()}

FEATURE_NAMES = [
    "n_points",
    "extent_x",
    "extent_y",
    "extent_z",
    "mean_z",
    "std_z",
    "mean_intensity",
    "std_intensity",
    "linearity",
    "planarity",
    "sphericity",
    "dominant_class_ratio",
]
