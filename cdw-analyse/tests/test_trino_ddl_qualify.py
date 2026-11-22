"""DDL runner qualifies ALTER TABLE to iceberg.defense (not session default)."""

from __future__ import annotations

import os
import unittest

from run_trino_ddl import transform_sql


class TestTrinoDdlQualify(unittest.TestCase):
    def setUp(self) -> None:
        self._old = {
            k: os.environ.get(k)
            for k in (
                "TRINO_QUALIFY_SCHEMA",
                "TRINO_SCHEMA",
                "TRINO_CATALOG",
                "TRINO_SKIP_CREATE_SCHEMA",
            )
        }
        os.environ["TRINO_QUALIFY_SCHEMA"] = "1"
        os.environ["TRINO_SCHEMA"] = "defense"
        os.environ["TRINO_CATALOG"] = "iceberg"
        os.environ["TRINO_SKIP_CREATE_SCHEMA"] = "1"

    def tearDown(self) -> None:
        for k, v in self._old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_alter_nba_incidents_qualified(self) -> None:
        raw = (
            "USE iceberg.defense;\n"
            "ALTER TABLE nba_incidents ADD COLUMN IF NOT EXISTS mission_readiness_json varchar;"
        )
        out = transform_sql(raw)
        self.assertIn(
            "ALTER TABLE iceberg.defense.nba_incidents", out, out
        )
        self.assertNotIn("iceberg.default", out.lower())

    def test_create_if_not_exists_qualified(self) -> None:
        raw = "CREATE TABLE IF NOT EXISTS nba_incidents (flow_uuid varchar);"
        out = transform_sql(raw)
        self.assertIn("CREATE TABLE IF NOT EXISTS iceberg.defense.nba_incidents", out)


if __name__ == "__main__":
    unittest.main()