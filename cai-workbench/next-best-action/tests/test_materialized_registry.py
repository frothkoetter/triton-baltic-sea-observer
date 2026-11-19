"""Phase 6 registry skip when Trino table missing."""

from __future__ import annotations

import unittest
from unittest import mock

from src.config import Settings
from src.materialize import MaterializeResult
from src.persistence import insert_materialized_tables, materialized_registry_ready


class TestMaterializedRegistry(unittest.TestCase):
    def test_registry_not_ready_on_missing_table(self):
        settings = Settings()
        with mock.patch("src.persistence.trino_client.fetch_all") as fetch:
            fetch.side_effect = RuntimeError(
                "TrinoUserError: TABLE_NOT_FOUND nba_materialized_tables"
            )
            self.assertFalse(materialized_registry_ready(settings))

    def test_insert_skips_when_registry_missing(self):
        settings = Settings()
        results = [
            MaterializeResult("ships", "nba_mat_x_ships", 1, 100),
        ]
        with mock.patch(
            "src.persistence.materialized_registry_ready", return_value=False
        ):
            with mock.patch("src.persistence.trino_client.execute_sql") as execute:
                insert_materialized_tables("flow-1", results, settings)
        execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()