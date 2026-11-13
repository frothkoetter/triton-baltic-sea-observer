#!/usr/bin/env python3
"""End-to-end spatial + AIS (+ pipeline WKT) checks against Trino."""

from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

import trino
from trino.auth import BasicAuthentication

ROOT = Path(__file__).resolve().parent.parent
QUERIES = ROOT / "sql" / "queries"
SAMPLE_CSV = ROOT / "data" / "sample_ais_positions.csv"
PIPELINE_TRACK_CSV = ROOT / "data" / "sample_ais_pipeline_track.csv"
BLACKOUT_CSV = ROOT / "data" / "sample_ais_blackout.csv"


def trino_conn():
    host = os.environ["TRINO_HOST"]
    port = int(os.environ.get("TRINO_PORT", "443"))
    user = os.environ["TRINO_USER"]
    password = os.environ.get("TRINO_PASSWORD")
    catalog = os.environ.get("TRINO_CATALOG", "iceberg")
    schema = os.environ.get("TRINO_SCHEMA", "defense")

    auth = BasicAuthentication(user, password) if password else None
    return trino.dbapi.connect(
        host=host,
        port=port,
        user=user,
        auth=auth,
        catalog=catalog,
        schema=schema,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "https"),
    )


def load_csv(cur, path: Path, delete_mmsi: tuple[int, ...]) -> int:
    if not path.is_file():
        print(f"Missing {path}", file=sys.stderr)
        return 0
    if delete_mmsi:
        ids = ", ".join(str(m) for m in delete_mmsi)
        cur.execute(f"DELETE FROM ais_events_ice WHERE mmsi IN ({ids})")
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        n = 0
        for row in reader:
            mmsi = int(row["mmsi"])
            dest = row.get("Destination") or ""
            dest_sql = f"'{dest.replace(chr(39), chr(39)+chr(39))}'" if dest else "NULL"
            cur.execute(
                f"""
                INSERT INTO ais_events_ice (mmsi, event_timestamp, latitude, longitude, speed, course, status, Destination)
                VALUES ({mmsi}, '{row["event_timestamp"]}', {float(row["latitude"])}, {float(row["longitude"])},
                        {float(row["speed"])}, {float(row["course"])}, '{row["status"]}', {dest_sql})
                """
            )
            n += 1
    return n


def run_sql_file(cur, path: Path) -> list:
    text = path.read_text(encoding="utf-8")
    stmt = text.strip().rstrip(";")
    cur.execute(stmt)
    return cur.fetchall()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--load-sample", action="store_true", help="INSERT sample_ais_positions.csv")
    parser.add_argument("--load-pipeline-track", action="store_true", help="INSERT sample_ais_pipeline_track.csv")
    parser.add_argument("--load-blackout-sample", action="store_true", help="INSERT sample_ais_blackout.csv")
    args = parser.parse_args()

    for key in ("TRINO_HOST", "TRINO_USER"):
        if not os.environ.get(key):
            print(f"Set {key} (and credentials) to run E2E.", file=sys.stderr)
            return 2

    conn = trino_conn()
    cur = conn.cursor()

    if args.load_sample:
        n = load_csv(
            cur,
            SAMPLE_CSV,
            (123456010, 123456011, 123456033, 123456001, 211234567),
        )
        print(f"Loaded {n} AIS rows from {SAMPLE_CSV.name}")

    if args.load_pipeline_track:
        n = load_csv(cur, PIPELINE_TRACK_CSV, (999888777,))
        print(f"Loaded {n} AIS rows from {PIPELINE_TRACK_CSV.name}")

    if args.load_blackout_sample:
        n = load_csv(cur, BLACKOUT_CSV, (888777666,))
        print(f"Loaded {n} AIS rows from {BLACKOUT_CSV.name}")

    try:
        cur.execute("SELECT COUNT(*) FROM ostsee_pipelines_segmented")
        (seg_count,) = cur.fetchone()
        print(f"ostsee_pipelines_segmented: {seg_count} segment row(s)")
    except Exception as exc:
        print(f"ostsee_pipelines_segmented: not ready ({exc})", file=sys.stderr)

    for name in (
        "03_ais_within_aoi.sql",
        "04_ais_near_harbour_km.sql",
        "05_pipeline_mehrfach_ueberquerungen.sql",
        "06_pipeline_korridor_langsamfahrt.sql",
        "07_korridor_segmented_performance.sql",
        "08_ais_blackout_pipelines.sql",
    ):
        path = QUERIES / name
        if not path.is_file():
            continue
        try:
            rows = run_sql_file(cur, path)
            print(f"{name}: {len(rows)} row(s)")
        except Exception as exc:
            print(f"{name}: ERROR {exc}", file=sys.stderr)

    try:
        cur.execute(
            """
            SELECT COUNT(*) FROM area_violation
            WHERE area_id = 'Bornholm Basin' AND transition IN ('enter', 'exit')
            """
        )
        (violations,) = cur.fetchone()
        print(f"area_violation (Bornholm Basin enter/exit): {violations} row(s)")
    except Exception as exc:
        print(f"area_violation: skip ({exc})", file=sys.stderr)

    print("E2E finished")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
