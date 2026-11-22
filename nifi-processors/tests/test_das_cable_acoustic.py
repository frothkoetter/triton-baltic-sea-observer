"""DAS cable acoustic engine and Balticconnector registry."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_UPLOAD = _ROOT / "upload"
sys.path.insert(0, _UPLOAD.as_posix())

from cable_corridor_registry import (  # noqa: E402
    cable_asset_keys,
    cable_length_m,
    cable_meta,
    interpolate_chainage,
    cable_vertices,
)
from das_acoustic_engine import (  # noqa: E402
    generate_das_batch,
    read_hdf5_features_from_bytes,
    write_hdf5_batch,
    THREAT_CLASSES,
)


class TestDasCableAcoustic(unittest.TestCase):
    def test_balticconnector_length_positive(self):
        length = cable_length_m("BALTICCONNECTOR")
        self.assertGreater(length, 50_000)
        meta = cable_meta("BALTICCONNECTOR")
        self.assertEqual(meta["name"], "Balticconnector")

    def test_chainage_on_cable(self):
        verts = cable_vertices("BALTICCONNECTOR")
        mid = cable_length_m("BALTICCONNECTOR") / 2
        pos = interpolate_chainage(verts, mid)
        self.assertTrue(23 < pos["lon"] < 25)
        self.assertTrue(59.3 < pos["lat"] < 60.0)

    def test_all_cable_assets_in_processor_list(self):
        keys = cable_asset_keys()
        self.assertEqual(len(keys), 7)
        self.assertIn("NORD_STREAM_2", keys)
        self.assertIn("PCABLE_BSH_4", keys)

    def test_each_cable_generates_batch(self):
        for key in cable_asset_keys():
            segments, meta = generate_das_batch(key, n_segments=2, n_channels=8, seed=0)
            self.assertEqual(len(segments), 2, msg=key)
            self.assertEqual(meta["cable_id"], cable_meta(key)["id"])

    def test_hdf5_roundtrip(self):
        segments, meta = generate_das_batch("BALTICCONNECTOR", n_segments=4, n_channels=16, seed=1)
        self.assertEqual(len(segments), 4)
        blob = write_hdf5_batch(segments, meta)
        self.assertGreater(len(blob), 500)
        rows = read_hdf5_features_from_bytes(blob)
        self.assertEqual(len(rows), 4)
        for r in rows:
            self.assertIn(r["threat_class"], THREAT_CLASSES)


if __name__ == "__main__":
    unittest.main()