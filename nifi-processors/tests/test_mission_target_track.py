"""Dynamic target tracking and STANAG mission tagging."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1] / "upload"
if _ROOT.as_posix() not in sys.path:
    sys.path.insert(0, _ROOT.as_posix())

from mission_target_track import (  # noqa: E402
    TrackSample,
    advance_target_sample,
    formation_slots,
    tag_stanag_message_body,
    track_should_continue,
)


class TestMissionTargetTrack(unittest.TestCase):
    def test_advance_target_dead_reckon(self):
        sample = TrackSample(latitude=54.0, longitude=11.0, cog_deg=90.0, sog_kn=10.0)
        nxt = advance_target_sample(sample)
        self.assertGreater(nxt.longitude, sample.longitude)

    def test_formation_slots_ahead_arc(self):
        center = TrackSample(54.5, 13.4, cog_deg=45.0, sog_kn=5.0)
        spec = {
            "LEADING_SEMICIRCLE": {
                "arc_degrees": 120,
                "offset_bearing": "ahead",
                "standoff_km_min": 8,
                "standoff_km_max": 12,
            }
        }
        slots = formation_slots(center, "LEADING_SEMICIRCLE", 3, spec)
        self.assertEqual(len(slots), 3)

    def test_stanag_footer_tags_mission_and_buoy(self):
        body = "MSGID/SPOT/001\nGENTEXT/\n"
        tagged = tag_stanag_message_body(
            body,
            mission_id="msn-abc-123",
            buoy_id="MAD-MSN-01",
            correlation_id="SONAR-42",
        )
        self.assertIn("msn-abc-123", tagged)
        self.assertIn("MAD-MSN-01", tagged)

    def test_track_duration_gate(self):
        self.assertTrue(track_should_continue(30, 240))
        self.assertFalse(track_should_continue(250, 240))


if __name__ == "__main__":
    unittest.main()