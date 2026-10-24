from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Iterator
from urllib.parse import urlparse

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


def iter_las_objects(s3_prefix: str, suffix: str = ".las") -> Iterator[LasObject]:
    import boto3

    bucket, prefix = _normalize_s3_uri(s3_prefix)
    session = boto3.session.Session()
    client = session.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-central-1"),
    )
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.lower().endswith(suffix):
                continue
            obj = client.get_object(Bucket=bucket, Key=key)
            body = obj["Body"].read()
            yield LasObject(key=key, s3_uri=f"s3://{bucket}/{key}", body=body)


def label_from_key(key: str) -> str | None:
    """Infer vessel type from filenames like sample_corvette.las."""
    name = key.rsplit("/", 1)[-1].lower()
    m = re.match(r"sample_([a-z_]+)\.las$", name)
    if m:
        return m.group(1)
    return None
