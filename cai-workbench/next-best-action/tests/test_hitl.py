"""Phase 4 sitrep link helper."""

from __future__ import annotations

import unittest

from src.hitl import sitrep_link_for_alert


class TestSitrepLink(unittest.TestCase):
    def test_flash_gps_jammer_link(self):
        link = sitrep_link_for_alert(
            {"source": "lagebild"},
            "77870eca-63e5-4af4-94af-5097340fe906",
            "FLASH",
        )
        self.assertIn("FLASH", link or "")
        self.assertIn("77870eca", link or "")

    def test_escalation_line(self):
        link = sitrep_link_for_alert(
            {
                "source": "sitrep_escalation",
                "comparison_line": "ESCALATED | Kiel",
            },
            "x",
            "FLASH",
        )
        self.assertIn("Kiel", link or "")


if __name__ == "__main__":
    unittest.main()