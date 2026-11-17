#!/usr/bin/env python3
"""List or fetch NATO SITREP PDFs from S3 or local Lagebild artifacts dir."""
from __future__ import annotations

import base64
import json
import os
import sys
from pathlib import Path
from urllib.parse import urlparse


def parse_prefix_uri(uri: str) -> tuple[str, str]:
    u = uri.strip()
    if u.startswith("s3a://"):
        u = "s3://" + u[6:]
    if not u.startswith("s3://"):
        raise ValueError("SITREP_S3_PREFIX must be s3:// or s3a://")
    parsed = urlparse(u)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix


def s3_client():
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from s3_boto import get_s3_client

    client, _mode = get_s3_client()
    return client


def s3_prefix() -> tuple[str, str] | None:
    uri = (os.environ.get("SITREP_S3_PREFIX") or os.environ.get("LAGEBILD_S3_PREFIX") or "").strip()
    if not uri:
        return None
    return parse_prefix_uri(uri)


def local_artifacts_dir() -> Path | None:
    raw = (
        os.environ.get("LAGEBILD_ARTIFACTS_DIR")
        or os.environ.get("SITREP_ARTIFACTS_DIR")
        or ""
    ).strip()
    if not raw:
        return None
    path = Path(raw)
    return path if path.is_dir() else None


def list_reports(limit: int = 50) -> dict:
    loc = local_artifacts_dir()
    if loc:
        index = loc / "index.json"
        if index.is_file():
            rows = json.loads(index.read_text(encoding="utf-8"))
            return {"ok": True, "source": "local", "rows": rows[:limit]}

    pref = s3_prefix()
    if not pref:
        return {"ok": False, "error": "Set SITREP_S3_PREFIX or LAGEBILD_ARTIFACTS_DIR"}
    bucket, prefix = pref
    client = s3_client()
    key = f"{prefix}index.json"
    obj = client.get_object(Bucket=bucket, Key=key)
    rows = json.loads(obj["Body"].read().decode("utf-8"))
    return {"ok": True, "source": "s3", "rows": rows[:limit], "index_key": key}


def fetch_pdf(pdf_name: str) -> dict:
    if ".." in pdf_name or "/" in pdf_name or not pdf_name.lower().endswith(".pdf"):
        return {"ok": False, "error": "Invalid pdf name"}

    loc = local_artifacts_dir()
    if loc:
        path = loc / pdf_name
        if path.is_file():
            data = path.read_bytes()
            return {
                "ok": True,
                "source": "local",
                "pdf": pdf_name,
                "content_base64": base64.b64encode(data).decode("ascii"),
                "size": len(data),
            }

    pref = s3_prefix()
    if not pref:
        return {"ok": False, "error": "SITREP not configured"}
    bucket, prefix = pref
    client = s3_client()
    key = f"{prefix}{pdf_name}"
    obj = client.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    max_mb = int(os.environ.get("SITREP_PDF_MAX_MB", "25"))
    if len(data) > max_mb * 1024 * 1024:
        return {"ok": False, "error": f"PDF exceeds {max_mb} MB limit"}
    return {
        "ok": True,
        "source": "s3",
        "pdf": pdf_name,
        "content_base64": base64.b64encode(data).decode("ascii"),
        "size": len(data),
    }


def main() -> None:
    if len(sys.argv) < 2:
        print(json.dumps({"ok": False, "error": "Usage: sitrep-bridge.py list|pdf <name>"}))
        sys.exit(1)
    cmd = sys.argv[1]
    try:
        if cmd == "list":
            out = list_reports()
        elif cmd == "pdf" and len(sys.argv) >= 3:
            out = fetch_pdf(sys.argv[2])
        else:
            out = {"ok": False, "error": "Usage: sitrep-bridge.py list|pdf <name>"}
        print(json.dumps(out))
        sys.exit(0 if out.get("ok") else 2)
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        sys.exit(2)


if __name__ == "__main__":
    main()