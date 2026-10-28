#!/usr/bin/env python3
"""
Export one UTM LAS sample per vessel type plus a combined catalog.

Output directory (default): nifi-processors/dev/samples/

  sample_oil_tanker.las
  sample_gas_tanker.las
  ...
  all_vessel_types_enu.las

Open in CloudCompare; color by Classification (1–9).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_UPLOAD = Path(__file__).resolve().parent.parent / "upload"
if str(_UPLOAD) not in sys.path:
    sys.path.insert(0, str(_UPLOAD))

import numpy as np

from LidarSimulator import _utm_anchor, vessel_spec_to_enu, write_las_enu
from lidar_vessel_geometry import VESSEL_TYPE_CODES, baltic_fleet_scene


def export_samples(
    out_dir: Path,
    base_lon: float = 19.0,
    base_lat: float = 59.0,
    seed: int = 42,
    grid_spacing_m: float = 400.0,
    catalog_heading_deg: float = 0.0,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    anchor_e, anchor_n = _utm_anchor(base_lon, base_lat)
    fleet = baltic_fleet_scene()

    catalog_pts: list[np.ndarray] = []
    catalog_cls: list[np.ndarray] = []

    for idx, spec in enumerate(fleet):
        code = VESSEL_TYPE_CODES[spec.vessel_type]
        # Single-vessel file at origin (heading north for consistent viewing).
        solo_rng = np.random.default_rng(seed + idx)
        solo = vessel_spec_to_enu(
            spec,
            solo_rng,
            anchor_e,
            anchor_n,
            heading_deg=catalog_heading_deg,
        )
        solo_path = out_dir / f"sample_{spec.vessel_type}.las"
        write_las_enu(str(solo_path), solo, np.full(len(solo), code, dtype=np.uint16))
        print(f"  {solo_path.name}  ({spec.name}, {len(solo):,} pts, class {code})")

        col, row = idx % 3, idx // 3
        offset_e = (col - 1) * grid_spacing_m
        offset_n = row * grid_spacing_m
        placed = vessel_spec_to_enu(
            spec,
            rng,
            anchor_e,
            anchor_n,
            offset_east=offset_e,
            offset_north=offset_n,
            heading_deg=catalog_heading_deg,
        )
        catalog_pts.append(placed)
        catalog_cls.append(np.full(len(placed), code, dtype=np.uint16))

    combined = np.vstack(catalog_pts)
    combined_cls = np.concatenate(catalog_cls)
    catalog_path = out_dir / "all_vessel_types_enu.las"
    write_las_enu(str(catalog_path), combined, combined_cls)
    print(f"  {catalog_path.name}  ({len(combined):,} pts, 3×3 grid @ {grid_spacing_m} m)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export LAS samples for each vessel type.")
    parser.add_argument(
        "-o",
        "--out-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "samples",
        help="Output directory (default: dev/samples)",
    )
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    print(f"Writing vessel samples to {args.out_dir}/")
    export_samples(args.out_dir, seed=args.seed)
    print("Done. Open all_vessel_types_enu.las or individual sample_*.las in CloudCompare.")


if __name__ == "__main__":
    main()
