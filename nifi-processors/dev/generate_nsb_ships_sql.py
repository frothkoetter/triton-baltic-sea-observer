#!/usr/bin/env python3
"""Emit INSERT rows for defense.ships (NSB trade fleet)."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "upload"))

from north_sea_baltic_fleet_registry import MAX_FLEET_SLOTS, fleet_unit_for_index


def sql_escape(s: str) -> str:
    return s.replace("'", "''")


def main() -> None:
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    count = min(count, MAX_FLEET_SLOTS)
    from north_sea_baltic_fleet_registry import MMSI_BASE

    print(f"-- NSB trade fleet ships (MMSI {MMSI_BASE}+), count={count}")
    print(
        "INSERT INTO ships (mmsi, ship_name, vessel_type, imo_number, call_sign, flag, "
        "length_m, beam_m, gross_tonnage, year_built, home_port) VALUES"
    )
    rows = []
    for i in range(count):
        u = fleet_unit_for_index(i)
        row = (
            f"({u['mmsi']}, '{sql_escape(u['name'])}', '{sql_escape(u['vessel_type'])}', "
            f"'{sql_escape(u['imo_number'])}', '{sql_escape(u['call_sign'])}', "
            f"'{sql_escape(u['flag'])}', {u['length_m']}, {u['beam_m']}, "
            f"{u['gross_tonnage']}, {u['year_built']}, '{sql_escape(u['home_port'])}')"
        )
        rows.append(row)
    print(",\n".join(rows) + ";")


if __name__ == "__main__":
    main()