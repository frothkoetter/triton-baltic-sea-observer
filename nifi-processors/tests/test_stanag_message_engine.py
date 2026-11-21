"""STANAG message engine — counts, routing, DTG, marine positions."""

from __future__ import annotations

import json
import re
import sys
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_UPLOAD = _ROOT / "upload"
for _p in (_UPLOAD.as_posix(), _ROOT.as_posix()):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from stanag_message_engine import (  # noqa: E402
    format_nato_dtg,
    format_position,
    generate_stanag_batch,
    render_ndjson,
    resolve_message_count,
)


SHIPS = [
    {"name": "FGS Sachsen", "mmsi": "MAR123404", "lat": 54.32, "lon": 11.8},
    {"name": "FGS Braunschweig", "mmsi": "MAR123411", "lat": 54.1, "lon": 12.2},
    {"name": "FGS Berlin", "mmsi": "MAR123437", "lat": 55.0, "lon": 15.5},
]


class TestStanagMessageEngine(unittest.TestCase):
    def test_dtg_includes_minutes(self):
        dtg = format_nato_dtg()
        self.assertRegex(dtg, r"^\d{6,7}Z[A-Z]{3}\d{2}$")

    def test_position_matches_marine_rounding(self):
        pos = format_position(54.190123, 12.035744)
        self.assertIn("54.19012", pos)
        self.assertIn("12.03574", pos)

    def test_major_exercise_volume(self):
        n = resolve_message_count(
            "major_exercise", 5, include_inter_unit=True, include_hq=True
        )
        self.assertGreaterEqual(n, 42)
        self.assertLessEqual(n, 80)

    def test_batch_has_mixed_directions(self):
        messages = generate_stanag_batch(
            SHIPS, count=30, exercise="BALTOPS", seed=42
        )
        self.assertEqual(len(messages), 30)
        directions = {m.traffic_direction for m in messages}
        self.assertIn("unit_to_hq", directions)
        self.assertIn("hq_to_unit", directions)
        self.assertIn("unit_to_unit", directions)

    def test_hq_and_unit_names_present(self):
        messages = generate_stanag_batch(SHIPS, count=20, exercise="SNMG-1 STEX", seed=1)
        for m in messages:
            self.assertTrue(m.message_from)
            self.assertTrue(m.message_to)
            self.assertIn("ZCZC", m.body)
            self.assertIn("NNNN", m.body)
            self.assertIn("POSITION/", m.body)
            if m.traffic_direction == "unit_to_hq":
                self.assertIn(m.message_from, [s["name"] for s in SHIPS])

    def test_ndjson_line_count(self):
        messages = generate_stanag_batch(SHIPS, count=8, exercise="OPEN GUARDIAN", seed=7)
        raw = render_ndjson(messages)
        lines = [ln for ln in raw.splitlines() if ln.strip()]
        self.assertEqual(len(lines), 8)
        row = json.loads(lines[0])
        self.assertIn("body_stanag", row)
        self.assertIn("traffic_direction", row)

    def test_msgid_pattern(self):
        messages = generate_stanag_batch(SHIPS, count=5, exercise="IRON WOLF", seed=3)
        for m in messages:
            self.assertRegex(m.message_id, r"^[A-Z]+/")


if __name__ == "__main__":
    unittest.main()