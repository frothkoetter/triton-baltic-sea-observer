#!/usr/bin/env python3
"""Fetch LAS from s3a:// (hadoop_file_url) and return downsampled points JSON."""
from __future__ import annotations

import json
import os
import sys
from io import BytesIO
from urllib.parse import urlparse


def parse_s3_uri(uri: str) -> tuple[str, str]:
    u = uri.strip()
    if u.startswith("s3a://"):
        u = "s3://" + u[6:]
    if not u.startswith("s3://"):
        raise ValueError("URI must be s3:// or s3a://")
    parsed = urlparse(u)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        raise ValueError("Invalid S3 URI (need bucket and key)")
    return bucket, key


def allowed_uri(uri: str) -> bool:
    prefix = os.environ.get("S3_LIDAR_ALLOWED_PREFIX", "").strip()
    if not prefix:
        return True
    norm = uri.replace("s3a://", "s3://")
    return norm.startswith(prefix.replace("s3a://", "s3://"))


def s3_client():
    import boto3

    access_key = (
      os.environ.get("S3_ACCESS_KEY_ID")
      or os.environ.get("S3_WORKLOAD_USER")
      or os.environ.get("AWS_ACCESS_KEY_ID")
    )
    secret_key = (
        os.environ.get("S3_SECRET_ACCESS_KEY")
        or os.environ.get("S3_WORKLOAD_PASSWORD")
        or os.environ.get("AWS_SECRET_ACCESS_KEY")
    )
    endpoint = os.environ.get("S3_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT_URL")
    region = os.environ.get("AWS_DEFAULT_REGION", "eu-central-1")

    if access_key and secret_key:
        return boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint or None,
            region_name=region,
        )

    mode = os.environ.get("LIDAR_USE_CML_S3", "auto").lower()
    if mode not in ("0", "false", "no"):
        try:
            import cml.data_v1 as cmldata

            conn = cmldata.get_connection(
                os.environ.get("LIDAR_S3_CONNECTION_NAME", "S3 Object Store")
            )
            return conn.get_base_connection()
        except Exception:
            if mode in ("1", "true", "yes"):
                raise

    raise RuntimeError(
        "Set S3_WORKLOAD_USER + S3_WORKLOAD_PASSWORD (or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)"
    )


def fetch_las_bytes(uri: str) -> bytes:
    if not allowed_uri(uri):
        raise ValueError("URI not allowed by S3_LIDAR_ALLOWED_PREFIX")
    bucket, key = parse_s3_uri(uri)
    client = s3_client()
    max_mb = int(os.environ.get("LIDAR_LAS_MAX_MB", "32"))
    head = client.head_object(Bucket=bucket, Key=key)
    size = int(head.get("ContentLength") or 0)
    if size > max_mb * 1024 * 1024:
        raise ValueError(f"LAS too large ({size} bytes, max {max_mb} MB)")
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


def points_from_las(data: bytes, limit: int = 1200) -> list[dict]:
    import laspy
    import numpy as np

    las = laspy.read(BytesIO(data))
    n = len(las.points)
    if n == 0:
        return []
    if n > limit:
        idx = np.linspace(0, n - 1, limit, dtype=int)
    else:
        idx = np.arange(n, dtype=int)
    xs = np.asarray(las.x)[idx]
    ys = np.asarray(las.y)[idx]
    zs = np.asarray(las.z)[idx]
    cls = np.asarray(las.classification)[idx]
    cx = float(np.median(xs))
    cy = float(np.median(ys))
    out = []
    for x, y, z, c in zip(xs, ys, zs, cls):
        # Scene-local metres-ish for SVG (WGS84 deltas scaled)
        out.append(
            {
                "x": float((x - cx) * 111320 * max(0.5, abs(np.cos(np.radians(cy))))),
                "y": float((y - cy) * 111320),
                "z": float(z),
                "cls": int(c),
            }
        )
    return out


def main() -> None:
    if len(sys.argv) < 2:
        print(json.dumps({"ok": False, "error": "Usage: lidar-las-bridge.py <s3a://...>"}))
        sys.exit(1)
    uri = sys.argv[1]
    try:
        raw = fetch_las_bytes(uri)
        points = points_from_las(raw)
        print(
            json.dumps(
                {
                    "ok": True,
                    "source": "s3",
                    "uri": uri,
                    "point_count": len(points),
                    "points": points,
                }
            )
        )
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc), "source": "s3", "uri": uri}))
        sys.exit(2)


if __name__ == "__main__":
    main()