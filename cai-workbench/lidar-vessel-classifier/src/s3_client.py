"""S3 client for training and inference: CML data connection or boto3."""

from __future__ import annotations

import os
from typing import Any

DEFAULT_CML_CONNECTION_NAME = "S3 Object Store"


def get_s3_client() -> Any:
    """
    Return a boto3-compatible S3 client.

    On Cloudera AI, uses ``cml.data_v1.get_connection`` (same as the workbench
    "S3 Object Store" connector). Elsewhere, uses boto3 with optional
    ``AWS_ENDPOINT_URL`` / ``AWS_DEFAULT_REGION``.
    """
    connection_name = os.environ.get(
        "LIDAR_S3_CONNECTION_NAME", DEFAULT_CML_CONNECTION_NAME
    )
    mode = os.environ.get("LIDAR_USE_CML_S3", "auto").lower()

    if mode in ("1", "true", "yes", "auto"):
        try:
            import cml.data_v1 as cmldata

            conn = cmldata.get_connection(connection_name)
            return conn.get_base_connection()
        except Exception as exc:
            if mode in ("1", "true", "yes"):
                raise RuntimeError(
                    f"Could not open CML connection '{connection_name}': {exc}"
                ) from exc

    import boto3

    session = boto3.session.Session()
    return session.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-central-1"),
    )
