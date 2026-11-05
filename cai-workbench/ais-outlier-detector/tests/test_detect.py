#!/usr/bin/env python3
"""Tests for AIS outlier detector."""

from __future__ import annotations

import json
import unittest

from src.detect import detect_outliers


class TestAisOutlierDetector(unittest.TestCase):
    def test_rendezvous(self) -> None:
        records = [
            {
                "MMSI": 123456006,
                "Event_Timestamp": "2025-10-14 17:05:54.044",
                "Latitude": 53.98346,
                "Longitude": 12.04434,
                "Speed": 8.0,
                "Course": 180.0,
                "Status": "Underway using engine",
                "Destination": "Primorsk",
            },
            {
                "MMSI": 123456007,
                "Event_Timestamp": "2025-10-14 17:05:54.044",
                "Latitude": 53.985,
                "Longitude": 12.046,
                "Speed": 7.0,
                "Course": 175.0,
                "Status": "Underway using engine",
                "Destination": "Primorsk",
            },
        ]
        out = detect_outliers({"records": records})
        self.assertTrue(out)
        types = {r["Outlier_Type"] for r in out}
        self.assertIn("Rendezvous", types)
        for row in out:
            self.assertIn("Reason", row)
            self.assertIn("Severity_Level", row)
            self.assertGreaterEqual(row["Severity_Level"], 0)
            self.assertLessEqual(row["Severity_Level"], 100)

    def test_misreported_moored_speed(self) -> None:
        records = [
            {
                "MMSI": 123456008,
                "Event_Timestamp": "2025-10-14 17:05:54.044",
                "Latitude": 54.0,
                "Longitude": 12.0,
                "Speed": 34.0,
                "Course": 180.0,
                "Status": "Moored",
                "Destination": "Primorsk",
            }
        ]
        out = detect_outliers({"records": records})
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["Outlier_Type"], "Misreported Status")

    def test_output_is_json_array_only(self) -> None:
        payload = detect_outliers({"records": []})
        text = json.dumps(payload)
        self.assertTrue(text.startswith("["))


if __name__ == "__main__":
    unittest.main()
