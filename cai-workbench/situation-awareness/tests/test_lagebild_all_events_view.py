#!/usr/bin/env python3
"""Static and offline checks for defense.lagebild_all_events (no live Trino)."""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
_REPO = _ROOT.parents[1]
sys.path.insert(0, str(_ROOT))

from src.config import Settings
from src.data_access import BALTIC_WIDE_LAGEBILD_ALL_EVENTS_SQL, default_lagebild_sql
from src.lagebild_viz_prep import normalize_lagebild_frame, rollup_data_sources


def _trino_all_events_section() -> str:
    sql_path = _REPO / "cdw-analyse" / "create_db_tables_trino.sql"
    text = sql_path.read_text(encoding="utf-8")
    start = text.index("CREATE OR REPLACE VIEW lagebild_all_events AS")
    end = text.index('ORDER BY harbour_name, "timestamp" DESC;', start)
    return text[start:end]


def _sql_lines_without_comments(section: str) -> list[str]:
    return [ln for ln in section.splitlines() if ln.strip() and not ln.strip().startswith("--")]


class TestLagebildAllEventsDdl(unittest.TestCase):
    def test_view_exists_and_keeps_lagebild_intact(self) -> None:
        text = (_REPO / "cdw-analyse" / "create_db_tables_trino.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("CREATE OR REPLACE VIEW lagebild AS", text)
        self.assertIn("CREATE OR REPLACE VIEW lagebild_all_events AS", text)
        snap_start = text.index("CREATE OR REPLACE VIEW lagebild AS")
        snap_end = text.index("CREATE OR REPLACE VIEW lagebild_all_events AS")
        snapshot = text[snap_start:snap_end]
        self.assertIn("WHERE rn = 1", snapshot)
        self.assertIn("GpsJammerLatestPerTile", snapshot)

    def test_all_events_drops_latest_dedupe(self) -> None:
        section = _trino_all_events_section()
        self.assertNotIn("GpsJammerLatestPerTile", section)
        self.assertIn("rn_nearest = 1", section)
        for line in _sql_lines_without_comments(section):
            if "rn_nearest" in line:
                continue
            self.assertNotRegex(
                line,
                r"\brn\s*=\s*1\b",
                msg=f"unexpected rn=1 in all-events view: {line}",
            )

    def test_comparison_sql_documents_both_views(self) -> None:
        cmp_path = _REPO / "cdw-analyse" / "lagebild_volume_compare.sql"
        body = cmp_path.read_text(encoding="utf-8")
        self.assertIn("lagebild_all_events", body)
        self.assertIn("defense.lagebild", body)


class TestLagebildAllEventsSettings(unittest.TestCase):
    def test_env_all_events_selects_view(self) -> None:
        prev = os.environ.get("LAGEBILD_ALL_EVENTS")
        prev_view = os.environ.get("LAGEBILD_VIEW")
        os.environ.pop("LAGEBILD_VIEW", None)
        os.environ["LAGEBILD_ALL_EVENTS"] = "1"
        try:
            sql = default_lagebild_sql(Settings())
            self.assertIn("lagebild_all_events", sql)
            self.assertIn(
                BALTIC_WIDE_LAGEBILD_ALL_EVENTS_SQL.split("{")[0].strip(),
                BALTIC_WIDE_LAGEBILD_ALL_EVENTS_SQL,
            )
        finally:
            if prev is None:
                os.environ.pop("LAGEBILD_ALL_EVENTS", None)
            else:
                os.environ["LAGEBILD_ALL_EVENTS"] = prev
            if prev_view is None:
                os.environ.pop("LAGEBILD_VIEW", None)
            else:
                os.environ["LAGEBILD_VIEW"] = prev_view

    def test_lagebild_view_override(self) -> None:
        prev = os.environ.get("LAGEBILD_VIEW")
        os.environ["LAGEBILD_VIEW"] = "lagebild_all_events"
        try:
            sql = default_lagebild_sql(Settings())
            self.assertIn("lagebild_all_events", sql)
        finally:
            if prev is None:
                os.environ.pop("LAGEBILD_VIEW", None)
            else:
                os.environ["LAGEBILD_VIEW"] = prev


class TestAllEventsVolumeOffline(unittest.TestCase):
    def test_stream_counts_exceed_latest_snapshot_simulation(self) -> None:
        """Same MMSI with multiple timestamps: stream keeps all rows after normalize."""
        snapshot = pd.DataFrame(
            {
                "Data_Source": ["AIS", "AIS", "Marine"],
                "ID": ["123", "456", "789"],
                "harbour_name": ["Kiel", "Kiel", "Kiel"],
                "details": ["pos", "pos", "status"],
            }
        )
        stream = pd.concat(
            [
                snapshot,
                pd.DataFrame(
                    {
                        "Data_Source": ["AIS", "AIS", "GPS_Jammer"],
                        "ID": ["123", "123", "u4pruyd"],
                        "harbour_name": ["Kiel", "Kiel", None],
                        "details": ["pos t-1h", "pos t-2h", "jam"],
                    }
                ),
            ],
            ignore_index=True,
        )
        norm_snap = normalize_lagebild_frame(snapshot)
        norm_stream = normalize_lagebild_frame(stream)
        self.assertLess(len(norm_snap), len(norm_stream))
        snap_roll = rollup_data_sources(norm_snap["data_source"])
        stream_roll = rollup_data_sources(norm_stream["data_source"])
        self.assertGreater(int(stream_roll.get("AIS", 0)), int(snap_roll.get("AIS", 0)))


if __name__ == "__main__":
    unittest.main()
