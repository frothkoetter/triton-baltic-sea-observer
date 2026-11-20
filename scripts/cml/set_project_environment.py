#!/usr/bin/env python3
"""
Set Cloudera AI (CML) project engine environment variables via API v2.

Docs: https://docs.cloudera.com/machine-learning/cloud/api/topics/ml-api-v2.html
Update project `environment`: see CML_AMP_APIv2 / update_project.

Usage:
  export CML_API_KEY='...'   # User Settings → API Keys (Audience: API)
  export CML_HOST='https://ml-xxxx.cloudera.site'

  python3 scripts/cml/set_project_environment.py \\
    --project triton-baltic-sea-observer \\
    --env-file scripts/cml/triton-project.env.local \\
    --dry-run

Secrets belong in the local env file only (never commit *.local.env).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

_SECRET_KEY = re.compile(
    r"(PASSWORD|SECRET|API_KEY|TOKEN|PRIVATE|ACCESS_KEY)", re.IGNORECASE
)


def _strip_host(host: str) -> str:
    host = host.strip().rstrip("/")
    if not host.startswith("http"):
        host = "https://" + host
    return host


def _parse_dotenv(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip("'\"")
        if key:
            out[key] = val
    return out


def _mask(key: str, value: str) -> str:
    if _SECRET_KEY.search(key):
        if len(value) <= 4:
            return "***"
        return value[:2] + "***" + value[-2:]
    return value


def _http_json(
    method: str,
    url: str,
    token: str,
    body: dict[str, Any] | None = None,
    *,
    ca_cert: str | None = None,
) -> Any:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    ctx = ssl.create_default_context()
    if ca_cert:
        ctx.load_verify_locations(ca_cert)
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=120) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} → HTTP {exc.code}: {detail}") from exc


def _list_projects(base: str, token: str, search: str, *, ca_cert: str | None) -> list[dict]:
    projects: list[dict] = []
    page_token: str | None = None
    while True:
        params: dict[str, str] = {"pageSize": "50"}
        if search:
            params["searchFilter"] = search
        if page_token:
            params["pageToken"] = page_token
        url = f"{base}/api/v2/projects?{urllib.parse.urlencode(params)}"
        payload = _http_json("GET", url, token, ca_cert=ca_cert)
        batch = payload.get("projects") if isinstance(payload, dict) else None
        if batch:
            projects.extend(batch)
        page_token = payload.get("nextPageToken") or payload.get("next_page_token")
        if not page_token:
            break
    return projects


def _resolve_project_id(
    base: str,
    token: str,
    *,
    project_id: str | None,
    project_name: str | None,
    ca_cert: str | None,
) -> tuple[str, str]:
    if project_id:
        p = _http_json("GET", f"{base}/api/v2/projects/{project_id}", token, ca_cert=ca_cert)
        name = p.get("name") or project_id
        return project_id, str(name)

    if not project_name:
        raise SystemExit("Provide --project-id or --project")

    # Exact name match first; fall back to searchFilter substring.
    hits = _list_projects(base, token, project_name, ca_cert=ca_cert)
    exact = [p for p in hits if (p.get("name") or "").lower() == project_name.lower()]
    if len(exact) == 1:
        return str(exact[0]["id"]), str(exact[0]["name"])
    if len(exact) > 1:
        raise SystemExit(f"Multiple projects named {project_name!r}")

    if len(hits) == 1:
        return str(hits[0]["id"]), str(hits[0]["name"])
    if not hits:
        raise SystemExit(f"No project matched search {project_name!r}")
    names = ", ".join(sorted({p.get("name", "?") for p in hits}))
    raise SystemExit(f"Ambiguous search {project_name!r}: {names}. Use --project-id.")


def _merge_env(existing: dict[str, str] | None, updates: dict[str, str]) -> dict[str, str]:
    merged = dict(existing or {})
    merged.update(updates)
    return merged


def _patch_via_rest(
    base: str,
    token: str,
    project_id: str,
    merged: dict[str, str],
    *,
    ca_cert: str | None,
    replace: bool,
) -> None:
    if replace:
        env = merged
    else:
        current = _http_json(
            "GET", f"{base}/api/v2/projects/{project_id}", token, ca_cert=ca_cert
        )
        raw = current.get("environment") if isinstance(current, dict) else None
        env = _merge_env(raw if isinstance(raw, dict) else None, merged)
    body = {"environment": env}

    # API accepts PATCH or PUT with partial Project body (see CML_AMP_APIv2).
    for method in ("PATCH", "PUT"):
        try:
            _http_json(
                method,
                f"{base}/api/v2/projects/{project_id}",
                token,
                body,
                ca_cert=ca_cert,
            )
            return
        except RuntimeError as exc:
            if "HTTP 404" in str(exc) or "HTTP 405" in str(exc):
                continue
            raise
    raise RuntimeError("PATCH and PUT both failed for update_project")


def _patch_via_cmlapi(
    base: str,
    token: str,
    project_id: str,
    merged: dict[str, str],
    *,
    replace: bool,
    ca_cert: str | None,
) -> None:
    import cmlapi  # type: ignore

    if ca_cert:
        client = cmlapi.default_client(base, token, ssl_ca_cert=ca_cert)
    else:
        client = cmlapi.default_client(base, token)
    if replace:
        env = merged
    else:
        proj = client.get_project(project_id=project_id)
        env = _merge_env(getattr(proj, "environment", None) or {}, merged)
    body = cmlapi.Project(environment=env)
    client.update_project(body, project_id)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cml-host", default=os.environ.get("CML_HOST", ""))
    parser.add_argument("--api-key", default=os.environ.get("CML_API_KEY", ""))
    parser.add_argument("--project", help="Project name (exact or search substring)")
    parser.add_argument("--project-id", help="Project UUID (skips name lookup)")
    parser.add_argument(
        "--env-file",
        type=Path,
        required=True,
        help="KEY=VALUE file (use triton-project.env.local, gitignored)",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace entire project environment with file contents (default: merge)",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--use-cmlapi", action="store_true", help="pip install HOST/api/v2/python.tar.gz")
    parser.add_argument("--ca-cert", default=os.environ.get("CML_CA_CERT", ""))
    args = parser.parse_args()

    if not args.api_key:
        print("Set CML_API_KEY or pass --api-key", file=sys.stderr)
        return 2
    if not args.cml_host:
        print("Set CML_HOST (https://your-workbench-domain) or pass --cml-host", file=sys.stderr)
        return 2
    if not args.env_file.is_file():
        print(f"Missing env file: {args.env_file}", file=sys.stderr)
        return 2

    base = _strip_host(args.cml_host)
    token = args.api_key.strip()
    updates = _parse_dotenv(args.env_file.resolve())
    if not updates:
        print("No variables in env file", file=sys.stderr)
        return 2

    ca = args.ca_cert.strip() or None
    pid, pname = _resolve_project_id(
        base, token, project_id=args.project_id, project_name=args.project, ca_cert=ca
    )

    print(f"Project: {pname} ({pid})")
    print(f"Variables to apply: {len(updates)}")
    for k in sorted(updates):
        print(f"  {k}={_mask(k, updates[k])}")

    if args.dry_run:
        print("Dry run — no API write.")
        return 0

    if args.use_cmlapi:
        _patch_via_cmlapi(
            base, token, pid, updates, replace=args.replace, ca_cert=ca
        )
    else:
        _patch_via_rest(
            base, token, pid, updates, ca_cert=ca, replace=args.replace
        )

    print("OK — project environment updated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())