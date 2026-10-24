from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Iterator
from urllib.parse import urlparse

LAS_MAGIC = b"LASF"
_SKIP_EXTENSIONS = frozenset(
    {".json", ".txt", ".csv", ".tsv", ".parquet", ".xml", ".html", ".md", ".zip", ".gz"}
)


def _normalize_s3_uri(uri: str) -> tuple[str, str]:
    uri = uri.strip()
    if uri.startswith("s3a://"):
        uri = "s3://" + uri[6:]
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// or s3a:// URI, got: {uri}")
    parsed = urlparse(uri)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix


@dataclass
class LasObject:
    key: str
    s3_uri: str
    body: bytes


def is_las_bytes(payload: bytes) -> bool:
    return len(payload) >= 4 and payload[:4] == LAS_MAGIC


def _basename(key: str) -> str:
    return key.rsplit("/", 1)[-1]


def _key_may_be_las(key: str, suffix: str = ".las") -> bool:
    """Include .las keys and extensionless objects (e.g. NiFi UUID keys)."""
    if not key or key.endswith("/"):
        return False
    name = _basename(key)
    if name.lower().endswith(suffix.lower()):
        return True
    if "." not in name:
        return True
    ext = "." + name.rsplit(".", 1)[-1].lower()
    return ext not in _SKIP_EXTENSIONS


def iter_las_objects(
    s3_prefix: str,
    suffix: str = ".las",
    *,
    require_las_magic: bool = True,
) -> Iterator[LasObject]:
    """
    List objects under an S3 prefix and yield LAS payloads.

    NiFi/CDP uploads often use keys without a .las extension; those are detected
    via the LAS file signature ``LASF`` (first four bytes).
    """
    from .s3_client import get_s3_client

    bucket, prefix = _normalize_s3_uri(s3_prefix)
    client = get_s3_client()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not _key_may_be_las(key, suffix=suffix):
                continue
            if require_las_magic and not key.lower().endswith(suffix.lower()):
                head = client.get_object(Bucket=bucket, Key=key, Range="bytes=0-3")[
                    "Body"
                ].read()
                if not is_las_bytes(head + b"\0" * max(0, 4 - len(head))):
                    continue
            obj = client.get_object(Bucket=bucket, Key=key)
            body = obj["Body"].read()
            if require_las_magic and not is_las_bytes(body):
                continue
            yield LasObject(key=key, s3_uri=f"s3://{bucket}/{key}", body=body)


def label_from_key(key: str) -> str | None:
    """Infer vessel type from filenames like sample_corvette.las."""
    name = key.rsplit("/", 1)[-1].lower()
    m = re.match(r"sample_([a-z_]+)\.las$", name)
    if m:
        return m.group(1)
    return None
