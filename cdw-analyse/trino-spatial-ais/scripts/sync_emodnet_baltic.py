#!/usr/bin/env python3
"""EMODnet WFS → bereinigtes WKT/Parquet (Draft sync_emodnet_baltic.py). Default: lokal in data/emodnet/."""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from shapely.geometry import mapping, shape
from shapely.ops import transform

ROOT = Path(__file__).resolve().parent.parent
BALTIC_BBOX = "9.0,53.5,30.5,66.0,EPSG:4326"
EMODNET_WFS_URL = "https://ows.emodnet-humanactivities.eu/wfs"
LAYERS = {
    "pipelines": "emodnet:pipelines",
    "cables": "emodnet:cables",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def fetch_wfs_geojson(type_name: str, bbox: str) -> dict:
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": type_name,
        "outputFormat": "application/json",
        "srsName": "EPSG:4326",
        "bbox": bbox,
    }
    log.info("WFS download: %s", type_name)
    response = requests.get(EMODNET_WFS_URL, params=params, timeout=120)
    response.raise_for_status()
    data = response.json()
    log.info("Features: %s", len(data.get("features", [])))
    return data


def force_2d(geom):
    if geom.has_z:
        return transform(lambda x, y, z=None: (x, y), geom)
    return geom


def normalize_features(geojson_data: dict, infra_category: str) -> list[dict]:
    cleaned: list[dict] = []
    download_ts = datetime.now(timezone.utc).isoformat()
    for feature in geojson_data.get("features", []):
        raw_geom = feature.get("geometry")
        props = feature.get("properties") or {}
        if not raw_geom:
            continue
        try:
            geom_obj = shape(raw_geom)
            if geom_obj.is_empty:
                continue
            geom_2d = force_2d(geom_obj)
            if not geom_2d.is_valid:
                geom_2d = geom_2d.buffer(0)
            minx, miny, maxx, maxy = geom_2d.bounds
            name = props.get("name") or props.get("cable_name") or props.get("pipeline_name") or "Unbekannt"
            medium = props.get("medium") or props.get("cable_type") or (
                "Telecom/Power" if infra_category == "cables" else "Unbekannt"
            )
            cleaned.append(
                {
                    "id": str(feature.get("id", "")),
                    "kategorie": infra_category,
                    "name": str(name).strip(),
                    "medium": str(medium).strip(),
                    "status": str(props.get("status") or "Unknown").strip(),
                    "operator": str(props.get("operator") or props.get("owner") or "").strip(),
                    "country": str(props.get("country") or "").strip(),
                    "geom_type": geom_2d.geom_type,
                    "wkt": geom_2d.wkt,
                    "min_lon": float(minx),
                    "min_lat": float(miny),
                    "max_lon": float(maxx),
                    "max_lat": float(maxy),
                    "updated_at": download_ts,
                }
            )
        except Exception as exc:
            log.warning("Skip feature %s: %s", feature.get("id"), exc)
    return cleaned


def write_parquet(records: list[dict], path: Path) -> None:
    schema = pa.schema(
        [
            ("id", pa.string()),
            ("kategorie", pa.string()),
            ("name", pa.string()),
            ("medium", pa.string()),
            ("status", pa.string()),
            ("operator", pa.string()),
            ("country", pa.string()),
            ("geom_type", pa.string()),
            ("wkt", pa.string()),
            ("min_lon", pa.float64()),
            ("min_lat", pa.float64()),
            ("max_lon", pa.float64()),
            ("max_lat", pa.float64()),
            ("updated_at", pa.string()),
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_table(table, path, compression="snappy")
    log.info("Wrote %s (%s rows)", path, len(records))


def write_compact_geojson(records: list[dict], path: Path) -> None:
    from shapely import wkt as shapely_wkt

    features = []
    for r in records:
        geom = shapely_wkt.loads(r["wkt"])
        features.append(
            {
                "type": "Feature",
                "id": r["id"],
                "properties": {
                    "kategorie": r["kategorie"],
                    "name": r["name"],
                    "medium": r["medium"],
                    "status": r["status"],
                    "operator": r["operator"],
                    "country": r["country"],
                },
                "geometry": mapping(geom),
            }
        )
    payload = {"type": "FeatureCollection", "features": features}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    log.info("Wrote %s", path)


def upload_s3(records: list[dict], bucket: str, prefix: str) -> None:
    import boto3

    s3 = boto3.client("s3", endpoint_url=os.environ.get("S3_ENDPOINT_URL"))
    tmp_dir = ROOT / "data" / "emodnet"
    tmp_parquet = tmp_dir / "_upload.parquet"
    tmp_geo = tmp_dir / "_upload.geojson"
    write_parquet(records, tmp_parquet)
    write_compact_geojson(records, tmp_geo)
    s3.upload_file(str(tmp_parquet), bucket, f"{prefix}/parquet/ostsee_infrastruktur.parquet")
    s3.upload_file(str(tmp_geo), bucket, f"{prefix}/geojson/ostsee_infrastruktur.geojson")
    log.info("Uploaded to s3://%s/%s", bucket, prefix)


def main() -> int:
    parser = argparse.ArgumentParser(description="EMODnet Baltic infrastructure → WKT/Parquet")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "data" / "emodnet")
    parser.add_argument("--bbox", default=BALTIC_BBOX)
    parser.add_argument("--upload-s3", action="store_true")
    parser.add_argument("--s3-bucket", default=os.environ.get("S3_BUCKET", "geodaten"))
    parser.add_argument("--s3-prefix", default=os.environ.get("S3_PREFIX", "ostsee/infrastruktur"))
    args = parser.parse_args()

    all_records: list[dict] = []
    for category, layer in LAYERS.items():
        try:
            raw = fetch_wfs_geojson(layer, args.bbox)
            all_records.extend(normalize_features(raw, category))
        except requests.RequestException as exc:
            log.error("WFS failed for %s: %s", layer, exc)
            if not all_records:
                return 1

    if not all_records:
        log.error("No features")
        return 1

    write_parquet(all_records, args.output_dir / "ostsee_infrastruktur.parquet")
    write_compact_geojson(all_records, args.output_dir / "ostsee_infrastruktur.geojson")

    if args.upload_s3:
        upload_s3(all_records, args.s3_bucket, args.s3_prefix)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
