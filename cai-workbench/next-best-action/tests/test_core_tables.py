"""Preflight when NBA_PERSIST_TRINO=1 but DDL not applied."""

from __future__ import annotations

import unittest
from unittest import mock

from src.config import Settings
from src.persistence import nba_core_tables_ready, require_nba_core_tables


class TestNbaCoreTables(unittest.TestCase):
    def test_not_ready_on_missing_incidents(self):
        settings = Settings()
        with mock.patch("src.persistence.trino_client.fetch_all") as fetch:
            fetch.side_effect = RuntimeError(
                "TrinoUserError(type=USER_ERROR, name=TABLE_NOT_FOUND, "
                "message=\"Table 'iceberg.defense.nba_incidents' does not exist\")"
            )
            self.assertFalse(nba_core_tables_ready(settings))

    def test_require_raises_with_ddl_hint(self):
        settings = Settings()
        with mock.patch("src.persistence.nba_core_tables_ready", return_value=False):
            with self.assertRaises(RuntimeError) as ctx:
                require_nba_core_tables(settings)
        self.assertIn("nba_incidents", str(ctx.exception))
        self.assertIn("nba_apply_all_ddl.sh", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()