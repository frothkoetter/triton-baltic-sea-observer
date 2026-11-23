"""DDL runner qualifies ALTER TABLE to iceberg.defense (not session default)."""

from __future__ import annotations

import os
import unittest

from run_trino_ddl import (
    _forgive_additive_ddl,
    _should_continue_on_error,
    session_schema,
    transform_sql,
)


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


    def test_forgive_table_already_exists_when_skip_drop(self) -> None:
        os.environ["TRINO_SKIP_DROP"] = "1"
        self.assertTrue(
            _forgive_additive_ddl(
                "CREATE TABLE iceberg.defense.nba_incidents (x varchar)",
                "TrinoUserError: TABLE_ALREADY_EXISTS",
            )
        )

    def test_continue_when_additive_or_skip_drop(self) -> None:
        os.environ.pop("TRINO_CONTINUE_ON_ERROR", None)
        os.environ["TRINO_SKIP_DROP"] = "1"
        self.assertTrue(_should_continue_on_error(False))

    def test_insert_ships_without_skip_create_schema(self) -> None:
        """Standalone ships_nsb_trade_1000.sql must not resolve to iceberg.default.ships."""
        saved = os.environ.copy()
        try:
            for key in (
                "TRINO_QUALIFY_SCHEMA",
                "TRINO_SKIP_CREATE_SCHEMA",
                "TRINO_CONNECT_SCHEMA",
                "TRINO_DDL_SESSION_SCHEMA",
            ):
                os.environ.pop(key, None)
            raw = "INSERT INTO ships (mmsi) VALUES (311040001);"
            out = transform_sql(raw)
            self.assertIn("INSERT INTO iceberg.defense.ships", out, out)
        finally:
            os.environ.clear()
            os.environ.update(saved)

    def test_session_schema_defaults_to_defense(self) -> None:
        saved = os.environ.get("TRINO_CONNECT_SCHEMA")
        saved_legacy = os.environ.get("TRINO_DDL_SESSION_SCHEMA")
        os.environ.pop("TRINO_CONNECT_SCHEMA", None)
        os.environ.pop("TRINO_DDL_SESSION_SCHEMA", None)
        os.environ["TRINO_SCHEMA"] = "defense"
        try:
            self.assertEqual(session_schema(for_ddl=True), "defense")
        finally:
            if saved is None:
                os.environ.pop("TRINO_CONNECT_SCHEMA", None)
            else:
                os.environ["TRINO_CONNECT_SCHEMA"] = saved
            if saved_legacy is None:
                os.environ.pop("TRINO_DDL_SESSION_SCHEMA", None)
            else:
                os.environ["TRINO_DDL_SESSION_SCHEMA"] = saved_legacy


if __name__ == "__main__":
    unittest.main()