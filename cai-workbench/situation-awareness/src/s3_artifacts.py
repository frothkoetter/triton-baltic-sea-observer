"""Upload Lagebild report artifacts to object storage (CDP workload / boto3)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


def s3_upload_enabled() -> bool:
    return os.environ.get("LAGEBILD_S3_UPLOAD", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _prefix_uri() -> str:
    uri = (
        os.environ.get("LAGEBILD_S3_PREFIX")
        or os.environ.get("SITREP_S3_PREFIX")
        or ""
    ).strip()
    if not uri:
        raise RuntimeError("Set LAGEBILD_S3_PREFIX (e.g. s3a://bucket/situation-awareness/)")
    return uri


def parse_prefix_uri(uri: str) -> tuple[str, str]:
    u = uri.strip()
    if u.startswith("s3a://"):
        u = "s3://" + u[6:]
    if not u.startswith("s3://"):
        raise ValueError("LAGEBILD_S3_PREFIX must be s3:// or s3a://")
    parsed = urlparse(u)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix


def s3_client():
    prefer_cml = os.environ.get("S3_USE_EXPLICIT_KEYS", "").strip().lower() not in (
        "1",
        "true",
        "yes",
    )
    if prefer_cml or os.environ.get("LAGEBILD_USE_CML_S3", "auto").lower() not in (
        "0",
        "false",
        "no",
    ):
        try:
            import cml.data_v1 as cmldata

            name = os.environ.get("LAGEBILD_S3_CONNECTION_NAME", "S3 Object Store")
            return cmldata.get_connection(name).get_base_connection()
        except Exception:
            if os.environ.get("LAGEBILD_USE_CML_S3", "auto").lower() in ("1", "true", "yes"):
                raise

    import boto3
    from botocore.config import Config

    access_key = (os.environ.get("S3_ACCESS_KEY_ID") or os.environ.get("S3_WORKLOAD_USER") or "").strip()
    secret_key = (
        os.environ.get("S3_SECRET_ACCESS_KEY") or os.environ.get("S3_WORKLOAD_PASSWORD") or ""
    ).strip()
    if not access_key or not secret_key:
        raise RuntimeError(
            "LAGEBILD_S3: use CML data connection 'S3 Object Store' or set CDP workload access key + secret "
            "(not Trino/LDAP password)"
        )
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=os.environ.get("S3_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT_URL") or None,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-central-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _object_key(prefix: str, name: str) -> str:
    return f"{prefix}{name}"


def upload_file(local_path: Path, bucket: str, key: str) -> str:
    client = s3_client()
    extra: dict[str, Any] = {}
    if local_path.suffix.lower() == ".pdf":
        extra["ContentType"] = "application/pdf"
    elif local_path.suffix.lower() == ".json":
        extra["ContentType"] = "application/json"
    elif local_path.suffix.lower() == ".png":
        extra["ContentType"] = "image/png"
    elif local_path.suffix.lower() == ".txt":
        extra["ContentType"] = "text/plain; charset=utf-8"
    client.upload_file(str(local_path), bucket, key, ExtraArgs=extra)
    scheme = os.environ.get("LAGEBILD_S3_URI_SCHEME", "s3a")
    return f"{scheme}://{bucket}/{key}"


def upload_report_artifacts(artifacts_dir: Path, meta: dict) -> dict:
    """
    Upload PDF companions and refresh index/latest on S3.
    Mutates and returns meta with s3_* URI fields.
    """
    if not s3_upload_enabled():
        return meta

    bucket, prefix = parse_prefix_uri(_prefix_uri())
    client = s3_client()

    names = [meta["pdf"], meta["text"]]
    for optional in ("map", "charts", "zebra"):
        n = meta.get(optional)
        if n:
            names.append(n)
    meta_json = artifacts_dir / meta["pdf"].replace(".pdf", ".json")
    if meta_json.is_file():
        names.append(meta_json.name)

    uris: dict[str, str] = {}
    for name in names:
        path = artifacts_dir / name
        if not path.is_file():
            continue
        key = _object_key(prefix, name)
        uris[name] = upload_file(path, bucket, key)

    index_path = artifacts_dir / "index.json"
    if index_path.is_file():
        index_key = _object_key(prefix, "index.json")
        uris["index.json"] = upload_file(index_path, bucket, index_key)

    latest = artifacts_dir / "latest.pdf"
    if latest.is_file():
        latest_key = _object_key(prefix, "latest.pdf")
        uris["latest.pdf"] = upload_file(latest, bucket, latest_key)

    if meta["pdf"] in uris:
        meta["s3_pdf_uri"] = uris[meta["pdf"]]
    meta["s3_prefix_uri"] = _prefix_uri()
    meta["s3_uris"] = uris

    stamp = meta.get("timestamp", "").replace(":", "-")
    manifest_key = _object_key(prefix, f"manifests/{stamp}.json")
    client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(meta, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    scheme = os.environ.get("LAGEBILD_S3_URI_SCHEME", "s3a")
    meta["s3_manifest_uri"] = f"{scheme}://{bucket}/{manifest_key}"

    return meta