"""Supplemental Trino DDL manifest lists real files under cdw-analyse."""

from __future__ import annotations

import unittest
from pathlib import Path

_CDW = Path(__file__).resolve().parents[1]
_ORDER = _CDW / "sql" / "trino_supplemental_ddl.order"


class TestSupplementalDdlOrder(unittest.TestCase):
    def test_all_paths_exist(self) -> None:
        self.assertTrue(_ORDER.is_file(), f"missing {_ORDER}")
        lines = _ORDER.read_text(encoding="utf-8").splitlines()
        paths = []
        for line in lines:
            line = line.split("#", 1)[0].strip()
            if line:
                paths.append(line)
        self.assertGreaterEqual(len(paths), 6, "expected NBA + vessel_route entries")
        for rel in paths:
            p = _CDW / rel
            self.assertTrue(p.is_file(), f"missing supplemental DDL: {rel}")


if __name__ == "__main__":
    unittest.main()