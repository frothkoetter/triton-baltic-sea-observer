from __future__ import annotations

import sys
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1] / "upload"
if _ROOT.as_posix() not in sys.path:
    sys.path.insert(0, _ROOT.as_posix())

from mission_buoy_playbook import build_mission_buoy_records  # noqa: E402
from mission_parallel import bundle_to_ndjson_lines  # noqa: E402


class TestMissionBuoyPlaybook(unittest.TestCase):
    def test_unique_buoy_ids_per_mission(self):
        examples = [
            {
                "type": "VESSEL",
                "classification": "CARGO",
                "confidence": 80,
                "estimatedDepth": 10,
                "motion": "MOVING",
                "gradient": "FLAT",
                "detectionConfidence": "HIGH",
            }
        ]
        r1 = build_mission_buoy_records(
            mission_id="aaa-bbbb-cccc-dddd",
            target_lat=54.5,
            target_lon=13.0,
            buoy_ids=["MAD-AAAA0001-01", "MAD-AAAA0001-02"],
            formations_spec={
                "PASSIVE_DENSE_RING": {
                    "arc_degrees": 360,
                    "offset_bearing": "omnidirectional",
                    "standoff_km_min": 5,
                    "standoff_km_max": 8,
                }
            },
            formation_key="PASSIVE_DENSE_RING",
            buoy_roles=None,
            object_examples=examples,
        )
        self.assertEqual(len(r1), 2)
        self.assertEqual(r1[0]["mission_id"], "aaa-bbbb-cccc-dddd")
        self.assertNotEqual(r1[0]["buoyid"], r1[1]["buoyid"])

    def test_bundle_split_lines(self):
        lines = bundle_to_ndjson_lines(
            {"missions": [{"mission.id": "m1"}, {"mission.id": "m2"}]}
        )
        self.assertEqual(len(lines), 2)


if __name__ == "__main__":
    unittest.main()