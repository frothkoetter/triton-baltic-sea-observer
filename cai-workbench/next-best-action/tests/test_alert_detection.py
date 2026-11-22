"""Phase 2 alert mapping and next_alert without Trino."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.alert_detector import (
    alert_from_das_cable_row,
    alert_from_lagebild_row,
    alert_from_pipeline_row,
)
from src.alert_rules import NoAlertFound, demo_greifswald_alert, next_alert
from src.config import Settings
from src import sitrep_bridge


class TestAlertMapping(unittest.TestCase):
    def test_pipeline_row_flash(self):
        alert = alert_from_pipeline_row(
            {
                "mmsi": "123",
                "pipeline_name": "Nord Stream branch",
                "medium": "gas",
                "verdachtsmuster": "Pipeline-Überquerung im Dark-Modus",
                "luecke_minuten": 90,
                "start_lat": 54.1,
                "start_lon": 13.2,
            }
        )
        self.assertEqual(alert.alert_type, "ais_pipeline_blackout")
        self.assertEqual(alert.priority, "FLASH")

    def test_das_cable_row_cui_flash(self):
        alert = alert_from_das_cable_row(
            {
                "cable_asset": "BALTICCONNECTOR",
                "cable_name": "Balticconnector",
                "threat_class": "cui_sabotage_tap",
                "predicted_threat_type": "cui_sabotage_tap",
                "chainage_m": 12000,
                "segment_lat": 59.7,
                "segment_lon": 21.1,
                "model_confidence": 0.9,
            }
        )
        self.assertEqual(alert.alert_type, "das_cable_threat")
        self.assertEqual(alert.priority, "FLASH")

    def test_lagebild_das_cable_branch(self):
        alert = alert_from_lagebild_row(
            {
                "Data_Source": "DAS_Cable",
                "ID": "das_bc.h5#2",
                "harbour_name": "Kiel",
                "latitude": 54.32,
                "longitude": 10.12,
                "details": "Cable: Balticconnector | Threat: anchor_drag",
            }
        )
        self.assertEqual(alert.alert_type, "das_cable_threat")
        self.assertEqual(alert.priority, "FLASH")

    def test_lagebild_gps_jammer(self):
        alert = alert_from_lagebild_row(
            {
                "Data_Source": "GPS_Jammer",
                "ID": "u33",
                "harbour_name": "Greifswald",
                "latitude": 54.09,
                "longitude": 13.45,
                "details": "Jamming: 1",
            }
        )
        self.assertEqual(alert.alert_type, "gps_jammer")
        self.assertEqual(alert.theatre, "Greifswald")


class TestNextAlertIdle(unittest.TestCase):
    def test_no_demo_raises_without_trino(self):
        settings = Settings()
        settings.demo_mode = False
        settings.dedupe_hours = 0
        with mock.patch("src.alert_rules.trino_client.trino_configured", return_value=False):
            with self.assertRaises(NoAlertFound):
                next_alert(settings)

    def test_detected_lagebild_via_mock(self):
        settings = Settings()
        settings.demo_mode = False
        settings.dedupe_hours = 0
        row = {
            "Data_Source": "RUS_Fleet",
            "ID": "273",
            "harbour_name": "Rostock",
            "latitude": 54.18,
            "longitude": 12.08,
            "details": "Unit: Frigate",
            "event_ts": "2026-01-01",
        }
        with mock.patch("src.alert_rules.trino_client.trino_configured", return_value=True):
            with mock.patch(
                "src.alert_detector.fetch_pipeline_blackout", return_value=None
            ):
                with mock.patch("src.alert_detector.alert_from_sitrep", return_value=None):
                    with mock.patch(
                        "src.alert_detector.fetch_lagebild_candidates",
                        return_value=[alert_from_lagebild_row(row)],
                    ):
                        with mock.patch(
                            "src.alert_rules.lagebild_flash_count", return_value=0
                        ):
                            alert = next_alert(settings)
        self.assertEqual(alert.alert_type, "rus_fleet_proximity")


class TestSitrepBridge(unittest.TestCase):
    def test_reads_escalated_line(self):
        with tempfile.TemporaryDirectory() as td:
            arts = Path(td)
            (arts / "sit.txt").write_text(
                "SITREP\n\nPer-Theme Delta\nESCALATED | AIS near Kiel harbour\n",
                encoding="utf-8",
            )
            (arts / "index.json").write_text(
                json.dumps(
                    [
                        {
                            "timestamp": "2026-11-17T10:00:00+00:00",
                            "text": "sit.txt",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            hit = sitrep_bridge.latest_escalated_theme(arts)
            self.assertIsNotNone(hit)
            theme, line = hit  # type: ignore[misc]
            self.assertIn("Kiel", theme)
            self.assertTrue(line.upper().startswith("ESCALATED"))


if __name__ == "__main__":
    unittest.main()