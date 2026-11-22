"""Resolve CDW Trino HTTP endpoint (host, port, statement path) from env."""

from __future__ import annotations

import base64
import os
import re
import ssl
from typing import List, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


def normalize_trino_coordinator_host(host: str) -> str:
    """DW overview host ``cdw-triton.*`` is not the Trino coordinator (``triton-trino.*``)."""
    if re.match(r"^cdw-triton\.", host, re.I):
        return re.sub(r"^cdw-triton\.", "triton-trino.", host, count=1, flags=re.I)
    return host


def apply_jdbc_url_env() -> None:
    """Populate TRINO_HOST/PORT/CATALOG/SCHEMA from TRINO_JDBC_URL when set."""
    url = os.environ.get("TRINO_JDBC_URL", "").strip()
    if not url:
        return
    m = re.match(
        r"^jdbc:trino://([^:/?#]+)(?::(\d+))?(?:/([^/?#]+))?(?:/([^/?#]+))?",
        url,
        re.IGNORECASE,
    )
    if not m:
        raise ValueError(f"Invalid TRINO_JDBC_URL: {url}")
    host, port, catalog, schema = m.groups()
    host = normalize_trino_coordinator_host(host)
    os.environ["TRINO_HOST"] = host
    if port:
        os.environ.setdefault("TRINO_PORT", port)
    if catalog:
        os.environ.setdefault("TRINO_CATALOG", catalog)
    if schema:
        os.environ.setdefault("TRINO_SCHEMA", schema)


def apply_trino_server_env() -> None:
    """
    TRINO_SERVER = URL from CDW **Copy Trino URL** (not JDBC, not DW overview link).
    Example: https://cdw-triton.env-hash.region.cloudera.site:443
    """
    server = os.environ.get("TRINO_SERVER", "").strip()
    if not server:
        return
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", server):
        server = "https://" + server
    parsed = urlparse(server)
    if parsed.hostname:
        os.environ["TRINO_HOST"] = normalize_trino_coordinator_host(parsed.hostname)
    if parsed.port:
        os.environ["TRINO_PORT"] = str(parsed.port)
    elif parsed.scheme == "https":
        os.environ.setdefault("TRINO_PORT", "443")
    if parsed.path and parsed.path not in ("", "/"):
        base = parsed.path.rstrip("/")
        os.environ["TRINO_STATEMENT_PATH"] = f"{base}/v1/statement"


def apply_all_trino_env() -> None:
    apply_jdbc_url_env()
    apply_trino_server_env()
    raw = os.environ.get("TRINO_HOST", "").strip()
    if raw:
        os.environ["TRINO_HOST"] = normalize_trino_coordinator_host(raw)


def apply_statement_path_to_client() -> None:
    """Mirror run_trino_ddl._apply_statement_path_override for dbapi clients."""
    path = os.environ.get("TRINO_STATEMENT_PATH", "").strip()
    if path:
        import trino.constants as trino_constants

        trino_constants.URL_STATEMENT_PATH = path


def statement_paths_to_try() -> List[str]:
    explicit = os.environ.get("TRINO_STATEMENT_PATH", "").strip()
    if explicit:
        return [explicit]
    return [
        "/v1/statement",
        "/trino/v1/statement",
        "/cloudera/trino/v1/statement",
    ]


def statement_post_url(host: str, port: str, path: str, use_tls: bool = True) -> str:
    scheme = "https" if use_tls else "http"
    return f"{scheme}://{host}:{port}{path}"


def probe_trino_http(
    *,
    timeout: float = 25.0,
) -> Tuple[bool, str, List[str]]:
    """
    Raw POST SELECT 1 without the Python dbapi. Returns (ok, working_url, log_lines).
    """
    apply_all_trino_env()
    host = os.environ.get("TRINO_HOST", "").strip()
    port = os.environ.get("TRINO_PORT", "443").strip()
    user = os.environ.get("TRINO_USER", "").strip()
    password = os.environ.get("TRINO_PASSWORD", "")
    catalog = os.environ.get("TRINO_CATALOG", "iceberg")
    schema = os.environ.get("TRINO_SCHEMA", "defense")
    if not host or not user:
        return False, "", ["Missing TRINO_HOST or TRINO_USER"]

    logs: List[str] = []
    ctx = ssl.create_default_context()
    auth = base64.b64encode(f"{user}:{password}".encode()).decode()

    for path in statement_paths_to_try():
        url = statement_post_url(host, port, path)
        req = Request(url, data=b"SELECT 1", method="POST")
        req.add_header("X-Trino-User", user)
        req.add_header("X-Trino-Catalog", catalog)
        req.add_header("X-Trino-Schema", schema)
        req.add_header("Authorization", f"Basic {auth}")
        req.add_header("Content-Type", "text/plain; charset=utf-8")
        try:
            with urlopen(req, timeout=timeout, context=ctx) as resp:
                snippet = resp.read(400).decode("utf-8", errors="replace")
                logs.append(f"HTTP {resp.status} POST {url} body[:200]={snippet[:200]!r}")
                if resp.status == 200 and "nextUri" in snippet:
                    if path != "/v1/statement":
                        os.environ["TRINO_STATEMENT_PATH"] = path
                        logs.append(f"Set TRINO_STATEMENT_PATH={path}")
                    return True, url, logs
                logs.append("200 but unexpected body (not Trino JSON?)")
        except HTTPError as exc:
            body = exc.read(300).decode("utf-8", errors="replace")
            logs.append(f"HTTP {exc.code} POST {url} body[:120]={body[:120]!r}")
            if exc.code == 401 and path == "/v1/statement":
                logs.append(
                    "Coordinator reachable; HTTP 401 = invalid user/password (not a DDL issue)."
                )
                return False, url, logs
        except URLError as exc:
            logs.append(f"URL error POST {url}: {exc}")

    return False, "", logs