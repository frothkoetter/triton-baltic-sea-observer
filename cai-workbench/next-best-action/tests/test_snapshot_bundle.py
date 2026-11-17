import unittest

from src.config import Settings
from src.snapshot_bundle import capture_bundle, offline_bundle


class TestSnapshotBundle(unittest.TestCase):
    def test_offline_bundle_tables(self):
        settings = Settings()
        settings.snapshot_tables = ["ships", "ais_events_ice"]
        bundle = offline_bundle("test-uuid", settings)
        names = {s.table_name for s in bundle.snapshots}
        self.assertEqual(names, {"ships", "ais_events_ice"})

    def test_capture_offline_when_configured(self):
        settings = Settings()
        settings.offline_snapshots = True
        bundle = capture_bundle("flow-1", settings)
        self.assertEqual(bundle.flow_uuid, "flow-1")