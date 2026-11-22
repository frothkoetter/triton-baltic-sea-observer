"""DAS cable acoustic engine and Balticconnector registry."""

from __future__ import annotations

import random
import sys
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_UPLOAD = _ROOT / "upload"
sys.path.insert(0, _UPLOAD.as_posix())

from cable_corridor_registry import (  # noqa: E402
    RANDOM_CABLE_ASSET,
    cable_asset_allowable_values,
    cable_asset_keys,
    cable_length_m,
    cable_meta,
    cable_vertices,
    interpolate_chainage,
    resolve_cable_asset,
)
from das_acoustic_engine import (  # noqa: E402
    MAX_WINDOW_S,
    batch_geolocation_attributes,
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
        allowable = cable_asset_allowable_values()
        self.assertEqual(len(allowable), 8)
        self.assertIn(RANDOM_CABLE_ASSET, allowable)

    def test_resolve_cable_asset_random_and_fixed(self):
        self.assertEqual(resolve_cable_asset("BALTICCONNECTOR"), "BALTICCONNECTOR")
        rng = random.Random(99)
        picked = {resolve_cable_asset(RANDOM_CABLE_ASSET, rng=rng) for _ in range(30)}
        self.assertTrue(picked.issubset(set(cable_asset_keys())))
        self.assertGreater(len(picked), 1)
        self.assertEqual(resolve_cable_asset(RANDOM_CABLE_ASSET, rng=random.Random(99)),
                         resolve_cable_asset(RANDOM_CABLE_ASSET, rng=random.Random(99)))

    def test_each_cable_generates_batch(self):
        for key in cable_asset_keys():
            segments, meta = generate_das_batch(key, n_segments=2, n_channels=8, seed=0)
            self.assertEqual(len(segments), 2, msg=key)
            self.assertEqual(meta["cable_id"], cable_meta(key)["id"])

    def test_batch_geolocation_attributes(self):
        segments, _ = generate_das_batch("BALTICCONNECTOR", n_segments=4, n_channels=8, seed=2)
        geo = batch_geolocation_attributes(segments)
        self.assertIn("latitude", geo)
        self.assertIn("longitude", geo)
        self.assertIn("das.segment.lat", geo)
        self.assertIn("das.segment.lon", geo)
        lat = float(geo["latitude"])
        lon = float(geo["longitude"])
        self.assertTrue(59.0 < lat < 60.5)
        self.assertTrue(23.0 < lon < 25.5)

    def test_max_window_30_seconds(self):
        segments, meta = generate_das_batch(
            "BALTICCONNECTOR",
            n_segments=2,
            n_channels=8,
            window_s=30.0,
            seed=3,
        )
        self.assertAlmostEqual(meta["window_s"], 30.0)
        self.assertEqual(int(segments[0].channels.shape[1]), int(30.0 * meta["fs_hz"]))

    def test_underwater_threat_synthesis(self):
        for threat in (
            "uboat_acoustic",
            "whale_vocalization",
            "anchor_slip",
            "hard_beep",
        ):
            segs, _ = generate_das_batch(
                "BALTICCONNECTOR",
                n_segments=1,
                threat_labels=[threat],
                window_s=8.0,
                n_channels=8,
                seed=4,
            )
            self.assertEqual(segs[0].threat_class, threat)
            self.assertGreater(float(segs[0].channels.std()), 0.01)

    def test_window_clamped_to_max(self):
        _, meta = generate_das_batch("BALTICCONNECTOR", window_s=120.0, n_segments=1, seed=1)
        self.assertAlmostEqual(meta["window_s"], MAX_WINDOW_S)

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