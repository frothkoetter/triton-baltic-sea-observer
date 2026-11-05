"""
Sparse Baltic sea-surface / wave points around a vessel (local frame, z-up, waterline z≈0).

Designed to add contextual water without dominating LAS size or ML labels (class 10).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np

from lidar_vessel_geometry import VesselSpec

# ASPRS extension in this project (vessel types use 1–9).
LAS_CLASS_WATER_SURFACE = 10


@dataclass(frozen=True)
class SeaSurfaceConfig:
    point_budget: int = 1_200
    margin_m: float = 0.0  # 0 → auto from vessel size
    wave_amplitude_m: float = 0.6
    wave_length_m: float = 18.0
    inner_clearance_m: float = 4.0  # hole around hull so water ring does not stack on hull


def _auto_margin(spec: VesselSpec) -> float:
    return max(40.0, min(180.0, 0.65 * max(spec.length_m, spec.beam_m)))


def _wave_height(x: np.ndarray, y: np.ndarray, amplitude: float, wavelength: float) -> np.ndarray:
    if amplitude <= 0 or wavelength <= 0:
        return np.zeros_like(x)
    k = 2.0 * np.pi / wavelength
    return amplitude * (0.55 * np.sin(k * x) + 0.45 * np.cos(0.85 * k * y))


def sample_sparse_sea_surface_local(
    spec: VesselSpec,
    rng: np.random.Generator,
    config: Optional[SeaSurfaceConfig] = None,
) -> np.ndarray:
    """
    Return (N, 3) points on a coarse sea grid with light swell, outside the hull footprint.

    N <= config.point_budget (often much less after inner hole).
    """
    cfg = config or SeaSurfaceConfig()
    budget = int(max(0, cfg.point_budget))
    if budget == 0:
        return np.empty((0, 3), dtype=np.float64)

    margin = cfg.margin_m if cfg.margin_m > 0 else _auto_margin(spec)
    x0, x1 = -spec.length_m / 2 - margin, spec.length_m / 2 + margin
    y0, y1 = -spec.beam_m / 2 - margin, spec.beam_m / 2 + margin

    aspect = (x1 - x0) / max(y1 - y0, 1.0)
    ny = int(max(8, round(np.sqrt(budget / max(aspect, 0.5)))))
    nx = int(max(8, round(budget / max(ny, 1))))
    nx = min(nx, budget)
    ny = min(ny, max(8, budget // max(nx, 1)))

    xs = np.linspace(x0, x1, nx)
    ys = np.linspace(y0, y1, ny)
    gx, gy = np.meshgrid(xs, ys)
    x = gx.ravel()
    y = gy.ravel()

    # Exclude interior near hull (ellipse in plan).
    rx = spec.length_m / 2 + cfg.inner_clearance_m
    ry = spec.beam_m / 2 + cfg.inner_clearance_m
    inside = (x / rx) ** 2 + (y / ry) ** 2 < 1.0
    x = x[~inside]
    y = y[~inside]
    if len(x) == 0:
        return np.empty((0, 3), dtype=np.float64)

    if len(x) > budget:
        idx = rng.choice(len(x), size=budget, replace=False)
        x, y = x[idx], y[idx]

    z = _wave_height(x, y, cfg.wave_amplitude_m, cfg.wave_length_m)
    z += rng.normal(0.0, 0.04, size=len(z))
    return np.column_stack((x, y, z))
