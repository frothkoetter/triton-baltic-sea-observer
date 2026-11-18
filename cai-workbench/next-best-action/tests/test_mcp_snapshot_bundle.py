import json
import unittest
from unittest.mock import MagicMock, patch

from src.config import Settings
from src.mcp_snapshot_bundle import (
    CAPTURE_SOURCE_JDBC_FALLBACK,
    CAPTURE_SOURCE_MCP,
    build_mcp_audit,
    capture_bundle_via_mcp,
    list_latest_snapshot_via_mcp,
)
from src.snapshot_bundle import SnapshotRef, capture_bundle


class TestMcpSnapshotBundle(unittest.TestCase):
    def test_build_mcp_audit(self):
        from datetime import datetime, timezone

        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
        audit = build_mcp_audit(
            flow_uuid="f-1",
            captured_at=ts,
            capture_source=CAPTURE_SOURCE_MCP,
            tables=[],
        )
        self.assertEqual(audit["flow_uuid"], "f-1")
        self.assertEqual(audit["capture_source"], CAPTURE_SOURCE_MCP)

    @patch("src.mcp_snapshot_bundle.latest_snapshot_for_table")
    @patch("src.mcp_snapshot_bundle._mcp_import_error", return_value="no package")
    def test_capture_falls_back_when_package_missing(self, _imp, mock_jdbc):
        mock_jdbc.return_value = SnapshotRef("ships", 99, None)
        settings = Settings()
        settings.snapshot_tables = ["ships"]
        bundle, audit = capture_bundle_via_mcp("flow-x", settings)
        self.assertEqual(bundle.snapshots[0].snapshot_id, 99)
        self.assertEqual(audit["capture_source"], CAPTURE_SOURCE_JDBC_FALLBACK)
        mock_jdbc.assert_called_once()

    @patch("src.mcp_snapshot_bundle.list_latest_snapshot_via_mcp")
    @patch("src.mcp_snapshot_bundle._mcp_import_error", return_value=None)
    def test_capture_via_mcp(self, _imp, mock_mcp):
        ref = SnapshotRef("ships", 42, "2025-06-01")
        mock_mcp.return_value = (ref, {"list_snapshots": {}, "table_health": {}})
        settings = Settings()
        settings.snapshot_tables = ["ships"]
        bundle, audit = capture_bundle_via_mcp("flow-y", settings)
        self.assertEqual(bundle.snapshots[0].snapshot_id, 42)
        self.assertEqual(audit["capture_source"], CAPTURE_SOURCE_MCP)

    @patch("src.mcp_snapshot_bundle._trino_env_for_mcp")
    def test_list_latest_snapshot_parses_json(self, mock_ctx):
        import sys
        import types

        mock_ctx.return_value.__enter__ = MagicMock(return_value=None)
        mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
        iceberg_sem = types.ModuleType("iceberg_semantics")
        iceberg_sem.list_snapshots = MagicMock(
            return_value=json.dumps(
                {
                    "table": "iceberg.defense.ships",
                    "snapshots": [{"snapshot_id": 7, "committed_at": "2025-01-01"}],
                }
            )
        )
        trino_tools = types.ModuleType("trino_tools")
        trino_tools.get_table_health = MagicMock(
            return_value=json.dumps({"table": "iceberg.defense.ships"})
        )
        tools_pkg = types.ModuleType("tools")
        tools_pkg.iceberg_semantics = iceberg_sem
        tools_pkg.trino_tools = trino_tools
        root = types.ModuleType("iceberg_mcp_server_trino")
        root.tools = tools_pkg
        with patch.dict(
            sys.modules,
            {
                "iceberg_mcp_server_trino": root,
                "iceberg_mcp_server_trino.tools": tools_pkg,
                "iceberg_mcp_server_trino.tools.iceberg_semantics": iceberg_sem,
                "iceberg_mcp_server_trino.tools.trino_tools": trino_tools,
            },
        ):
            ref, _detail = list_latest_snapshot_via_mcp("ships", Settings())
        self.assertEqual(ref.snapshot_id, 7)

    def test_capture_bundle_flag_offline(self):
        settings = Settings()
        settings.offline_snapshots = True
        settings.use_iceberg_mcp = True
        cap = capture_bundle("flow-z", settings)
        self.assertIsNone(cap.mcp_audit)


if __name__ == "__main__":
    unittest.main()