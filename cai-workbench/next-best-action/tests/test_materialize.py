"""Phase 6 materialize naming and offline mode."""

from __future__ import annotations

import unittest

from src.config import Settings
from src.materialize import mat_table_name, materialize_bundle
from src.snapshot_bundle import offline_bundle


class TestMaterialize(unittest.TestCase):
    def test_mat_table_name_safe(self):
        name = mat_table_name("a1b2c3d4-e5f6-7890-abcd-ef1234567890", "ais_events_ice")
        self.assertTrue(name.startswith("nba_mat_"))
        self.assertIn("ais_events_ice", name)

    def test_offline_materialize_stub(self):
        settings = Settings()
        settings.offline_snapshots = True
        bundle = offline_bundle("flow-1", settings)
        results = materialize_bundle(bundle, settings, row_limit=100)
        self.assertEqual(len(results), len(settings.snapshot_tables))


if __name__ == "__main__":
    unittest.main()