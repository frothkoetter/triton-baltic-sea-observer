"""SOCMINT scenario catalog and NiFi message shape."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_UPLOAD = _ROOT / "upload"
for _p in (_UPLOAD.as_posix(), _ROOT.as_posix()):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from socmint_catalog import (  # noqa: E402
    TRUST_DISINFO,
    generate_socmint_batch,
    scenario_pool,
)


class TestSocmintCatalog(unittest.TestCase):
    def test_pool_includes_baltiysk_and_disinfo(self) -> None:
        ids = {s.scenario_id for s in scenario_pool()}
        self.assertIn("baltiysk_ad_geoloc", ids)
        self.assertIn("baltiysk_fake_iran_strike_photo", ids)
        self.assertIn("eagle_s_estlink_ais", ids)

    def test_batch_schema_fields(self) -> None:
        rows = generate_socmint_batch(20, seed=42)
        self.assertEqual(len(rows), 20)
        required = {
            "user_name",
            "user_username",
            "tweet",
            "ts",
            "priority",
            "latitude",
            "longitude",
            "platform",
            "trust_tag",
            "disinformation_flag",
            "verification_notes",
            "scenario_id",
        }
        for row in rows:
            self.assertTrue(required.issubset(row.keys()))
            self.assertGreaterEqual(row["latitude"], 53.5)
            self.assertLessEqual(row["latitude"], 65.8)

    def test_disinformation_scenario_tagged(self) -> None:
        rows = generate_socmint_batch(200, seed=7)
        disinfo = [r for r in rows if r["scenario_id"] == "baltiysk_fake_iran_strike_photo"]
        self.assertTrue(disinfo)
        self.assertTrue(all(r["disinformation_flag"] for r in disinfo))
        self.assertTrue(all(r["trust_tag"] == TRUST_DISINFO for r in disinfo))
        self.assertIn("Qeshm", disinfo[0]["verification_notes"])

    def test_ndjson_line_length(self) -> None:
        lines = [json.dumps(r) for r in generate_socmint_batch(3, seed=1)]
        for line in lines:
            self.assertLessEqual(len(line), 8000)


if __name__ == "__main__":
    unittest.main()