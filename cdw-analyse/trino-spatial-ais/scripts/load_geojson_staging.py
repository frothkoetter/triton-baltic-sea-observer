#!/usr/bin/env python3
"""Lädt data/mock_ostsee_featurecollection.geojson in raw_ostsee_geojson (nach sql/04 DDL)."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import trino
from trino.auth import BasicAuthentication

ROOT = Path(__file__).resolve().parent.parent
GEOJSON = ROOT / "data" / "mock_ostsee_featurecollection.geojson"


def main() -> int:
    for key in ("TRINO_HOST", "TRINO_USER"):
        if not os.environ.get(key):
            print(f"Set {key} for Trino insert.", file=sys.stderr)
            return 2
    if not GEOJSON.is_file():
        print(f"Missing {GEOJSON}", file=sys.stderr)
        return 1

    payload = GEOJSON.read_text(encoding="utf-8").strip().replace("'", "''")
    conn = trino.dbapi.connect(
        host=os.environ["TRINO_HOST"],
        port=int(os.environ.get("TRINO_PORT", "443")),
        user=os.environ["TRINO_USER"],
        catalog=os.environ.get("TRINO_CATALOG", "iceberg"),
        schema=os.environ.get("TRINO_SCHEMA", "defense"),
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "https"),
        auth=BasicAuthentication(os.environ["TRINO_USER"], os.environ.get("TRINO_PASSWORD", "")),
    )
    cur = conn.cursor()
    cur.execute("DELETE FROM raw_ostsee_geojson WHERE quelle = 'file_upload'")
    cur.execute(
        f"INSERT INTO raw_ostsee_geojson (json_payload, quelle) VALUES ('{payload}', 'file_upload')"
    )
    print(f"Inserted GeoJSON from {GEOJSON.name}; re-run sql/04 CTAS or scripts/rebuild_pipelines.sh")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
