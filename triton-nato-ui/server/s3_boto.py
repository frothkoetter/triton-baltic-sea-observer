#!/usr/bin/env python3
"""Shared boto3 client for CDP object store (CML data connection or workload keys)."""
from __future__ import annotations

import os


def _strip(v: str | None) -> str:
    return (v or "").strip()


def explicit_keys_configured() -> bool:
    ak = _strip(
        os.environ.get("S3_ACCESS_KEY_ID")
        or os.environ.get("S3_WORKLOAD_USER")
        or os.environ.get("AWS_ACCESS_KEY_ID")
    )
    sk = _strip(
        os.environ.get("S3_SECRET_ACCESS_KEY")
        or os.environ.get("S3_WORKLOAD_PASSWORD")
        or os.environ.get("AWS_SECRET_ACCESS_KEY")
    )
    return bool(ak and sk)


def prefer_cml_data_connection() -> bool:
    if _strip(os.environ.get("S3_USE_EXPLICIT_KEYS")).lower() in ("1", "true", "yes"):
        return False
    pref = _strip(os.environ.get("S3_PREFER_CML_CONNECTION", "auto")).lower()
    if pref in ("1", "true", "yes"):
        return True
    if pref in ("0", "false", "no"):
        return False
    # On Cloudera AI, default to project data connection (not LDAP / Trino passwords).
    return bool(os.environ.get("CDSW_PROJECT_ID") or os.environ.get("CDSW_WORKING_DIRECTORY"))


def connection_name() -> str:
    return (
        _strip(os.environ.get("SITREP_S3_CONNECTION_NAME"))
        or _strip(os.environ.get("LIDAR_S3_CONNECTION_NAME"))
        or _strip(os.environ.get("S3_CONNECTION_NAME"))
        or "S3 Object Store"
    )


def boto_config():
    from botocore.config import Config

    return Config(signature_version="s3v4", s3={"addressing_style": "path"})


def client_from_explicit_keys():
    import boto3

    access_key = _strip(
        os.environ.get("S3_ACCESS_KEY_ID")
        or os.environ.get("S3_WORKLOAD_USER")
        or os.environ.get("AWS_ACCESS_KEY_ID")
    )
    secret_key = _strip(
        os.environ.get("S3_SECRET_ACCESS_KEY")
        or os.environ.get("S3_WORKLOAD_PASSWORD")
        or os.environ.get("AWS_SECRET_ACCESS_KEY")
    )
    session_token = _strip(os.environ.get("AWS_SESSION_TOKEN"))
    if not access_key or not secret_key:
        return None
    endpoint = _strip(os.environ.get("S3_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT_URL")) or None
    region = _strip(os.environ.get("AWS_DEFAULT_REGION")) or "eu-central-1"
    kwargs = {
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "endpoint_url": endpoint,
        "region_name": region,
        "config": boto_config(),
    }
    if session_token:
        kwargs["aws_session_token"] = session_token
    return boto3.client("s3", **kwargs)


def client_from_cml_connection():
    import cml.data_v1 as cmldata

    conn = cmldata.get_connection(connection_name())
    return conn.get_base_connection()


def get_s3_client():
    """
    Prefer CML **S3 Object Store** data connection on CAWorkbench unless
    S3_USE_EXPLICIT_KEYS=1. Avoids using Trino/LDAP passwords as S3 keys.
    """
    errors: list[str] = []
    if prefer_cml_data_connection():
        try:
            return client_from_cml_connection(), "cml"
        except Exception as exc:
            errors.append(f"CML data connection '{connection_name()}': {exc}")

    client = client_from_explicit_keys()
    if client is not None:
        return client, "explicit"

    if not prefer_cml_data_connection():
        try:
            return client_from_cml_connection(), "cml"
        except Exception as exc:
            errors.append(f"CML fallback: {exc}")

    hint = (
        "Configure project Data Connection 'S3 Object Store' OR set "
        "S3_ACCESS_KEY_ID + S3_SECRET_ACCESS_KEY (CDP workload access key — not Trino/LDAP password)."
    )
    if errors:
        raise RuntimeError(f"{hint} Details: {'; '.join(errors)}")
    raise RuntimeError(hint)


def diagnose_s3(prefix_uri: str | None = None) -> dict:
    """Lightweight permission check (no secrets in response)."""
    try:
        client, mode = get_s3_client()
    except Exception as exc:
        return {"ok": False, "error": str(exc), "mode": None}

    out = {"ok": True, "mode": mode, "connection": connection_name()}
    if prefix_uri:
        from urllib.parse import urlparse

        u = prefix_uri.replace("s3a://", "s3://")
        parsed = urlparse(u)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        if bucket:
            try:
                if key:
                    client.head_object(Bucket=bucket, Key=key)
                    out["head_object"] = f"s3://{bucket}/{key}"
                else:
                    client.head_bucket(Bucket=bucket)
                    out["head_bucket"] = bucket
            except Exception as exc:
                out["ok"] = False
                out["error"] = str(exc)
    return out


if __name__ == "__main__":
    import json
    import sys

    prefix = sys.argv[1] if len(sys.argv) > 1 else None
    print(json.dumps(diagnose_s3(prefix)))