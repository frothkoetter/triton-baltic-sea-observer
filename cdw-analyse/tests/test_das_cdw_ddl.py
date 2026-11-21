#!/usr/bin/env python3
"""Static checks for DAS CDW / Lagebild / outlier DDL (no live Trino)."""

from __future__ import annotations

import re
import unittest
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_TRINO = _REPO / "cdw-analyse" / "create_db_tables_trino.sql"
_IMPALA = _REPO / "cdw-analyse" / "create_db_tables_impala.sql"
_OUTLIER = _REPO / "cdw-analyse" / "outlier_detection_trino.sql"


class TestDasCdwDdl(unittest.TestCase):
    def test_trino_table_and_lagebild_union(self) -> None:
        text = _TRINO.read_text(encoding="utf-8")
        self.assertIn("CREATE TABLE das_cable_spectrogram_metadata", text)
        self.assertGreaterEqual(text.count("'DAS_Cable' AS Data_Source"), 2)
        self.assertIn("defense.das_cable_spectrogram_metadata", text)
        self.assertIn("FilteredDasCableSegments", text)

    def test_impala_and_hive_parity(self) -> None:
        imp = _IMPALA.read_text(encoding="utf-8")
        self.assertIn("das_cable_spectrogram_metadata", imp)
        hive = (_REPO / "cdw-analyse" / "create_db_tables_hive.sql").read_text(encoding="utf-8")
        self.assertIn("das_cable_spectrogram_metadata", hive)

    def test_outlier_case5(self) -> None:
        body = _OUTLIER.read_text(encoding="utf-8")
        self.assertIn("DAS Cable Threat", body)
        self.assertIn("das_cable_spectrogram_metadata", body)

    def test_nba_sql_template(self) -> None:
        sql = (
            _REPO
            / "cai-workbench"
            / "next-best-action"
            / "sql"
            / "alerts"
            / "das_cable_threat_top1.sql"
        ).read_text(encoding="utf-8")
        self.assertIn("{qualified}.das_cable_spectrogram_metadata", sql)
        self.assertIn("cui_sabotage_tap", sql)

    def test_ontology_mapping_file(self) -> None:
        m = _REPO / "ontology" / "mappings" / "das_cable_spectrogram_metadata.yaml"
        self.assertTrue(m.is_file())
        body = m.read_text(encoding="utf-8")
        self.assertIn("DasCableSpectrogramMetadata", body)
        self.assertIn("das.cable.asset", body)

    def test_ui_query_matches_columns(self) -> None:
        q = (_REPO / "triton-ui" / "server" / "queries.js").read_text(encoding="utf-8")
        sample = (_REPO / "triton-ui" / "server" / "sample-data.js").read_text(encoding="utf-8")
        for col in ("segment_index", "cable_asset", "threat_class", "segment_lat"):
            self.assertIn(col, q)
            self.assertIn(col, sample)


if __name__ == "__main__":
    unittest.main()