"""
Parametric marine vessel point clouds in a vessel-fixed frame.

Axes (meters):
  x — bow (+) / stern (-)
  y — starboard (+) / port (-)
  z — up (+), waterline at z = 0

Points are intended for surface sampling (hull and superstructure), not solid fills.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np

PointCloud = np.ndarray  # (N, 3) float64


@dataclass(frozen=True)
class VesselSpec:
    vessel_type: str
    name: str
    length_m: float
    beam_m: float
    height_m: float
    point_count: int
    heading_deg: float = 0.0


def _rng(seed: Optional[int] = None) -> np.random.Generator:
    return np.random.default_rng(seed)


def _concat(parts: List[PointCloud]) -> PointCloud:
    parts = [p for p in parts if p is not None and len(p)]
    if not parts:
        return np.empty((0, 3), dtype=np.float64)
    return np.vstack(parts)


def _sample_rectangle(n: int, rng: np.random.Generator, u_axis: str, v_axis: str,
                      u_range: Tuple[float, float], v_range: Tuple[float, float],
                      fixed: Dict[str, float]) -> PointCloud:
    u_lo, u_hi = u_range
    v_lo, v_hi = v_range
    u = rng.uniform(u_lo, u_hi, n)
    v = rng.uniform(v_lo, v_hi, n)
    coords = {"x": np.zeros(n), "y": np.zeros(n), "z": np.zeros(n)}
    coords[u_axis] = u
    coords[v_axis] = v
    for axis, value in fixed.items():
        coords[axis][:] = value
    return np.column_stack((coords["x"], coords["y"], coords["z"]))


def _sample_box_surface(n: int, rng: np.random.Generator,
                        x0: float, x1: float, y0: float, y1: float, z0: float, z1: float) -> PointCloud:
    """Uniform over the six faces of an axis-aligned box."""
    if n <= 0:
        return np.empty((0, 3), dtype=np.float64)
    faces = []
    counts = rng.multinomial(n, [1 / 6] * 6)
    faces.append(_sample_rectangle(counts[0], rng, "y", "z", (y0, y1), (z0, z1), {"x": x0}))
    faces.append(_sample_rectangle(counts[1], rng, "y", "z", (y0, y1), (z0, z1), {"x": x1}))
    faces.append(_sample_rectangle(counts[2], rng, "x", "z", (x0, x1), (z0, z1), {"y": y0}))
    faces.append(_sample_rectangle(counts[3], rng, "x", "z", (x0, x1), (z0, z1), {"y": y1}))
    faces.append(_sample_rectangle(counts[4], rng, "x", "y", (x0, x1), (y0, y1), {"z": z0}))
    faces.append(_sample_rectangle(counts[5], rng, "x", "y", (x0, x1), (y0, y1), {"z": z1}))
    return _concat(faces)


def _sample_hull_like(n: int, rng: np.random.Generator, length: float, beam: float, draft: float,
                      freeboard: float) -> PointCloud:
    """
    Smooth hull: elliptic cross-section that tapers at bow/stern, deck at z=freeboard.
    """
    if n <= 0:
        return np.empty((0, 3), dtype=np.float64)
    x = rng.uniform(-length / 2, length / 2, n)
    t = np.abs(x) / (length / 2)
    taper = np.clip(1.0 - t ** 1.8, 0.08, 1.0)
    half_beam = (beam / 2) * taper
    theta = rng.uniform(0, 2 * np.pi, n)
    y = half_beam * np.sin(theta)
    z_scale = np.where(theta >= 0, freeboard, draft)
    z = z_scale * np.cos(theta)
    # Flat deck band near the top
    deck = (z > freeboard * 0.85) & (np.abs(y) < half_beam * 0.92)
    z[deck] = freeboard
    return np.column_stack((x, y, z))


def _sample_cylinder(n: int, rng: np.random.Generator, length: float, radius: float,
                     axis: str = "x", center: Tuple[float, float, float] = (0.0, 0.0, 0.0)) -> PointCloud:
    if n <= 0:
        return np.empty((0, 3), dtype=np.float64)
    u = rng.uniform(-length / 2, length / 2, n)
    theta = rng.uniform(0, 2 * np.pi, n)
    r = radius * (0.98 + 0.02 * rng.random(n))
    cx, cy, cz = center
    if axis == "x":
        x, y, z = u + cx, r * np.cos(theta) + cy, r * np.sin(theta) + cz
    elif axis == "z":
        x, y, z = r * np.cos(theta) + cx, r * np.sin(theta) + cy, u + cz
    else:
        x, y, z = r * np.cos(theta) + cx, u + cy, r * np.sin(theta) + cz
    return np.column_stack((x, y, z))


def _sample_sphere(n: int, rng: np.random.Generator, radius: float,
                   center: Tuple[float, float, float]) -> PointCloud:
    if n <= 0:
        return np.empty((0, 3), dtype=np.float64)
    cx, cy, cz = center
    phi = rng.uniform(0, np.pi, n)
    theta = rng.uniform(0, 2 * np.pi, n)
    x = cx + radius * np.sin(phi) * np.cos(theta)
    y = cy + radius * np.sin(phi) * np.sin(theta)
    z = cz + radius * np.cos(phi)
    return np.column_stack((x, y, z))


def _superstructure(length: float, beam: float, base_x: float, width_ratio: float, height: float,
                    n: int, rng: np.random.Generator) -> PointCloud:
    w = beam * width_ratio
    y0, y1 = -w / 2, w / 2
    x0, x1 = base_x - length / 2, base_x + length / 2
    return _sample_box_surface(n, rng, x0, x1, y0, y1, 0.0, height)


def build_oil_tanker(spec: VesselSpec, rng: np.random.Generator) -> PointCloud:
    L, B, H = spec.length_m, spec.beam_m, spec.height_m
    n = spec.point_count
    n_hull = int(n * 0.55)
    n_deck = int(n * 0.15)
    n_house = int(n * 0.12)
    n_pipes = n - n_hull - n_deck - n_house
    hull = _sample_hull_like(n_hull, rng, L, B, draft=H * 0.45, freeboard=H * 0.22)
    deck = _sample_rectangle(n_deck, rng, "x", "y", (-L * 0.42, L * 0.38), (-B * 0.42, B * 0.42),
                             {"z": H * 0.22})
    house = _superstructure(L * 0.12, B, base_x=L * 0.32, width_ratio=0.55, height=H * 0.55, n=n_house, rng=rng)
    pipes = _sample_cylinder(n_pipes, rng, length=L * 0.04, radius=B * 0.04, axis="z",
                             center=(L * 0.05, 0.0, H * 0.35))
    return _concat([hull, deck, house, pipes])


def build_gas_tanker(spec: VesselSpec, rng: np.random.Generator) -> PointCloud:
    L, B, H = spec.length_m, spec.beam_m, spec.height_m
    n = spec.point_count
    n_hull = int(n * 0.45)
    n_tanks = int(n * 0.35)
    n_house = int(n * 0.12)
    n_deck = n - n_hull - n_tanks - n_house
    hull = _sample_hull_like(n_hull, rng, L, B, draft=H * 0.42, freeboard=H * 0.2)
    deck = _sample_rectangle(n_deck, rng, "x", "y", (-L * 0.4, L * 0.35), (-B * 0.4, B * 0.4),
                             {"z": H * 0.2})
    house = _superstructure(L * 0.1, B, base_x=L * 0.34, width_ratio=0.5, height=H * 0.5, n=n_house, rng=rng)
    tanks = []
    per = max(1, n_tanks // 4)
    for i, xc in enumerate(np.linspace(-L * 0.22, L * 0.18, 4)):
        tanks.append(_sample_sphere(per, rng, radius=B * 0.22, center=(xc, 0.0, H * 0.38 + i * 0.02)))
    return _concat([hull, deck, house, _concat(tanks)])


def build_container_ship(spec: VesselSpec, rng: np.random.Generator) -> PointCloud:
    L, B, H = spec.length_m, spec.beam_m, spec.height_m
    n = spec.point_count
    n_hull = int(n * 0.4)
    n_bridge = int(n * 0.1)
    n_stacks = n - n_hull - n_bridge
    hull = _sample_hull_like(n_hull, rng, L, B, draft=H * 0.35, freeboard=H * 0.18)
    bridge = _superstructure(L * 0.08, B, base_x=-L * 0.38, width_ratio=0.65, height=H * 0.75, n=n_bridge, rng=rng)
    stacks = []
    rows, cols = 8, 6
    cell = min(L * 0.55 / cols, B * 0.7 / rows)
    per_cell = max(1, n_stacks // (rows * cols))
    x0 = -L * 0.05
    for r in range(rows):
        for c in range(cols):
            bx = x0 + c * cell * 1.05
            by = -B * 0.32 + r * cell * 1.05
            tier_h = rng.integers(2, 6)
            stacks.append(_sample_box_surface(per_cell, rng, bx, bx + cell * 0.9, by, by + cell * 0.85,
                                              H * 0.18, H * 0.18 + tier_h * cell * 0.9))
    return _concat([hull, bridge, _concat(stacks)])


def build_ferry(spec: VesselSpec, rng: np.random.Generator) -> PointCloud:
    L, B, H = spec.length_m, spec.beam_m, spec.height_m
    n = spec.point_count
    n_hull = int(n * 0.35)
    n_house = int(n * 0.45)
    n_ramp = n - n_hull - n_house
    hull = _sample_hull_like(n_hull, rng, L, B * 1.05, draft=H * 0.3, freeboard=H * 0.2)
    house = _superstructure(L * 0.55, B, base_x=L * 0.05, width_ratio=0.88, height=H * 0.85, n=n_house, rng=rng)
    ramp = _sample_rectangle(n_ramp, rng, "x", "y", (L * 0.38, L * 0.48), (-B * 0.35, B * 0.35),
                             {"z": H * 0.12})
    return _concat([hull, house, ramp])


def build_sailing_vessel(spec: VesselSpec, rng: np.random.Generator) -> PointCloud:
    L, B, H = spec.length_m, spec.beam_m, spec.height_m
    n = spec.point_count
    n_hull = int(n * 0.5)
    n_mast = int(n * 0.25)
    n_boom = n - n_hull - n_mast
    hull = _sample_hull_like(n_hull, rng, L, B, draft=H * 0.35, freeboard=H * 0.12)
    mast = _sample_cylinder(n_mast, rng, length=H * 0.95, radius=B * 0.04, axis="z",
                            center=(L * 0.02, 0.0, H * 0.12))
    boom = _sample_cylinder(n_boom, rng, length=L * 0.35, radius=B * 0.025, axis="x",
                            center=(L * 0.05, 0.0, H * 0.55))
    return _concat([hull, mast, boom])


def build_corvette(spec: VesselSpec, rng: np.random.Generator) -> PointCloud:
    """Steregushchiy-class scale (~104 m) — stealthy superstructure, helipad aft."""
    L, B, H = spec.length_m, spec.beam_m, spec.height_m
    n = spec.point_count
    n_hull = int(n * 0.5)
    n_super = int(n * 0.25)
    n_mast = int(n * 0.1)
    n_pad = n - n_hull - n_super - n_mast
    hull = _sample_hull_like(n_hull, rng, L, B, draft=H * 0.35, freeboard=H * 0.18)
    super_ = _superstructure(L * 0.35, B, base_x=-L * 0.05, width_ratio=0.7, height=H * 0.55, n=n_super, rng=rng)
    mast = _sample_box_surface(n_mast, rng, -L * 0.02, L * 0.04, -B * 0.05, B * 0.05, H * 0.55, H * 0.85)
    pad = _sample_rectangle(n_pad, rng, "x", "y", (L * 0.18, L * 0.42), (-B * 0.28, B * 0.28),
                            {"z": H * 0.2})
    return _concat([hull, super_, mast, pad])


def build_frigate(spec: VesselSpec, rng: np.random.Generator) -> PointCloud:
    """Admiral Grigorovich-class scale (~124 m)."""
    L, B, H = spec.length_m, spec.beam_m, spec.height_m
    n = spec.point_count
    n_hull = int(n * 0.48)
    n_super = int(n * 0.28)
    n_stack = int(n * 0.12)
    n_fore = n - n_hull - n_super - n_stack
    hull = _sample_hull_like(n_hull, rng, L, B, draft=H * 0.38, freeboard=H * 0.2)
    super_ = _superstructure(L * 0.22, B, base_x=L * 0.08, width_ratio=0.62, height=H * 0.62, n=n_super, rng=rng)
    stack = _sample_box_surface(n_stack, rng, L * 0.02, L * 0.08, -B * 0.12, B * 0.12, H * 0.45, H * 0.78)
    forecastle = _sample_box_surface(n_fore, rng, -L * 0.48, -L * 0.28, -B * 0.35, B * 0.35, H * 0.18, H * 0.32)
    return _concat([hull, super_, stack, forecastle])


def build_submarine(spec: VesselSpec, rng: np.random.Generator) -> PointCloud:
    """Kilo-class scale — cigar hull, sail, horizontal planes."""
    L, B, H = spec.length_m, spec.beam_m, spec.height_m
    radius = B / 2
    n = spec.point_count
    n_hull = int(n * 0.65)
    n_sail = int(n * 0.2)
    n_planes = n - n_hull - n_sail
    hull = _sample_cylinder(n_hull, rng, length=L * 0.92, radius=radius, axis="x")
    hull[:, 2] -= H * 0.35  # submerged bias
    sail = _sample_box_surface(n_sail, rng, L * 0.02, L * 0.12, -radius * 0.35, radius * 0.35,
                               -H * 0.05, H * 0.45)
    planes = _sample_rectangle(n_planes, rng, "x", "y", (-L * 0.08, L * 0.08), (-radius * 1.6, radius * 1.6),
                               {"z": -H * 0.15})
    return _concat([hull, sail, planes])


def build_landing_ship(spec: VesselSpec, rng: np.random.Generator) -> PointCloud:
    """Ropucha-class LST scale — boxy hull, bow ramp block."""
    L, B, H = spec.length_m, spec.beam_m, spec.height_m
    n = spec.point_count
    n_hull = int(n * 0.55)
    n_cargo = int(n * 0.25)
    n_ramp = n - n_hull - n_cargo
    hull = _sample_hull_like(n_hull, rng, L, B, draft=H * 0.32, freeboard=H * 0.22)
    cargo = _superstructure(L * 0.45, B, base_x=L * 0.05, width_ratio=0.85, height=H * 0.35, n=n_cargo, rng=rng)
    ramp = _sample_box_surface(n_ramp, rng, -L * 0.48, -L * 0.35, -B * 0.4, B * 0.4, H * 0.05, H * 0.35)
    return _concat([hull, cargo, ramp])


BUILDERS: Dict[str, Callable[[VesselSpec, np.random.Generator], PointCloud]] = {
    "oil_tanker": build_oil_tanker,
    "gas_tanker": build_gas_tanker,
    "container_ship": build_container_ship,
    "ferry": build_ferry,
    "sailing": build_sailing_vessel,
    "corvette": build_corvette,
    "frigate": build_frigate,
    "submarine": build_submarine,
    "landing_ship": build_landing_ship,
}


def baltic_fleet_scene() -> List[VesselSpec]:
    """Realistic order-of-magnitude dimensions; positions are applied by the simulator."""
    return [
        VesselSpec("oil_tanker", "Baltic Aframax", 245, 42, 28, 120_000, heading_deg=12),
        VesselSpec("gas_tanker", "LNG Carrier", 290, 46, 32, 130_000, heading_deg=95),
        VesselSpec("container_ship", "Feedermax", 180, 28, 35, 110_000, heading_deg=270),
        VesselSpec("ferry", "Car Ferry", 160, 26, 40, 90_000, heading_deg=180),
        VesselSpec("sailing", "Sailing Yacht", 18, 5, 22, 8_000, heading_deg=45),
        VesselSpec("corvette", "Corvette (Project 20380)", 104, 13, 18, 45_000, heading_deg=210),
        VesselSpec("frigate", "Frigate (Project 11356)", 124, 15, 22, 55_000, heading_deg=300),
        VesselSpec("submarine", "Diesel Sub (Kilo-class)", 73, 10, 12, 40_000, heading_deg=135),
        VesselSpec("landing_ship", "LST (Ropucha-class)", 112, 15, 16, 42_000, heading_deg=75),
    ]


def vessel_local_points(spec: VesselSpec, rng: Optional[np.random.Generator] = None) -> PointCloud:
    rng = rng or _rng()
    builder = BUILDERS.get(spec.vessel_type)
    if builder is None:
        raise ValueError(f"Unknown vessel_type: {spec.vessel_type}")
    return builder(spec, rng)
