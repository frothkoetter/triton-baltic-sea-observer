#!/usr/bin/env python3
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from src.harbour_geography import (  # noqa: E402
    country_for_harbour,
    enrich_harbour_columns,
    geography_context_block,
    geography_warnings,
)


class HarbourGeographyTests(unittest.TestCase):
    def test_baltiysk_is_russia(self) -> None:
        self.assertEqual(country_for_harbour("Baltiysk"), "Russia")

    def test_kiel_is_germany(self) -> None:
        self.assertEqual(country_for_harbour("Kiel"), "Germany")

    def test_enrich_columns(self) -> None:
        df = pd.DataFrame(
            {"harbour_name": ["Baltiysk", "Kiel"], "Data_Source": ["RUS_Fleet", "AIS"]}
        )
        out = enrich_harbour_columns(df)
        self.assertEqual(out.loc[0, "harbour_country"], "Russia")
        self.assertEqual(out.loc[0, "harbour_affiliation"], "ADVERSARY")
        self.assertEqual(out.loc[1, "harbour_affiliation"], "ALLIED")

    def test_context_mentions_baltiysk_not_german(self) -> None:
        block = geography_context_block(enrich_harbour_columns(pd.DataFrame({"harbour_name": ["Baltiysk"]})))
        self.assertIn("NOT German", block)
        self.assertIn("Baltiysk", block)

    def test_detect_german_baltiysk_hallucination(self) -> None:
        bad = "During the window, German Baltic harbours--primarily Baltiysk--show activity."
        self.assertTrue(geography_warnings(bad))

    def test_clean_text_no_warning(self) -> None:
        ok = "Adversary RUS Fleet activity at Baltiysk (Russia); allied sector Greifswald (Germany) routine."
        self.assertFalse(geography_warnings(ok))


if __name__ == "__main__":
    unittest.main()